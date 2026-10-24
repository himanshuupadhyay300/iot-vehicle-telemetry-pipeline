# Architecture

All diagrams are Mermaid and render natively on GitHub.

## 1. End-to-end data flow

```mermaid
flowchart LR
    subgraph SRC["Sources (synthetic)"]
        F1["CSV / JSON-lines files<br/>landing folder or ADLS 'landing'"]
        F2["Azure Event Hubs<br/>(optional streaming)"]
    end

    subgraph ORCH["Orchestration"]
        ADF["Azure Data Factory<br/>(template)"]
        CLI["python -m src.pipeline<br/>(local, implemented)"]
    end

    subgraph LAKE["Lakehouse: Parquet locally / Delta Lake on ADLS Gen2"]
        B[("BRONZE<br/>raw text + lineage<br/>append-only")]
        S[("SILVER<br/>typed, validated, deduped<br/>MERGE on vehicle_id + event_ts")]
        Q[("SILVER REJECTS<br/>quarantine + failed rules")]
        G1[("GOLD<br/>vehicle_daily_summary")]
        G2[("GOLD<br/>temperature_alerts")]
        G3[("GOLD<br/>vehicle_kpis")]
    end

    subgraph SERVE["Serving"]
        SQL["Spark SQL / Databricks SQL<br/>(sql/gold_queries.sql)"]
        PBI["Power BI<br/>(optional)"]
    end

    F1 --> CLI
    ADF -. triggers notebook .-> CLI
    F2 -. foreachBatch .-> B
    CLI --> B --> S
    S -->|invalid rows| Q
    S --> G1 & G2
    G1 --> G3
    S --> G3
    G1 & G2 & G3 --> SQL --> PBI

    STATE[["state.json<br/>file checksums + stage status"]]
    CLI <--> STATE
    DQ["Data quality report<br/>+ execution summary<br/>(JSON + Markdown)"]
    S --> DQ
    CLI --> DQ
```

## 2. Incremental run with late and duplicate events

```mermaid
sequenceDiagram
    autonumber
    participant L as Landing folder
    participant P as Pipeline
    participant St as State store
    participant B as Bronze
    participant S as Silver
    participant G as Gold

    L->>P: batch_001.csv, batch_002.json
    P->>St: has_file(sha256)?
    St-->>P: batch_001 already ingested, batch_002 is new
    P->>B: append batch_002 (all columns as text + lineage)
    P->>St: register batch, stage=bronze
    P->>S: validate, quarantine bad rows
    Note over P,S: de-dupe inside the batch (latest ingested copy wins)
    P->>S: compare with Silver: flag late arrivals, count already-seen keys
    P->>S: UPSERT on (vehicle_id, event_ts), partition = event_date
    P->>St: stage=silver, affected_dates=[...]
    P->>G: recompute ONLY affected dates (partition replace)
    P->>G: rebuild KPIs
    P->>St: stage=gold
    Note over P,St: crash between stages? the next run resumes the pending stage
```

## 3. Gold data model

```mermaid
erDiagram
    SILVER_TELEMETRY ||--o{ GOLD_VEHICLE_DAILY_SUMMARY : "aggregated by vehicle_id + event_date"
    SILVER_TELEMETRY ||--o{ GOLD_TEMPERATURE_ALERTS : "engine_temp_c >= warning"
    GOLD_VEHICLE_DAILY_SUMMARY ||--|| GOLD_VEHICLE_KPIS : "rolled up by vehicle_id"

    SILVER_TELEMETRY {
        string vehicle_id PK
        timestamp event_ts PK
        date event_date "partition"
        double speed_kmph
        double engine_temp_c
        double fuel_level_pct
        double battery_voltage_v
        double latitude
        double longitude
        boolean is_late_arrival
        array dq_warnings
    }
    GOLD_VEHICLE_DAILY_SUMMARY {
        string vehicle_id PK
        date event_date PK
        long record_count
        double distance_km
        double moving_minutes
        double max_engine_temp_c
        long data_gap_segments
        long gps_jump_segments
        long late_arrival_records
    }
    GOLD_TEMPERATURE_ALERTS {
        string alert_id PK
        string vehicle_id
        timestamp event_ts
        string severity "WARNING or CRITICAL"
        double engine_temp_c
    }
    GOLD_VEHICLE_KPIS {
        string vehicle_id PK
        long total_records
        long active_days
        double total_distance_km
        double temp_alert_rate_per_1000
    }
```

## 4. What runs where

| Component | Local (no Azure) | Azure |
|---|---|---|
| Bronze / Silver / Gold (PySpark) | Implemented and tested | Same code, `storage.format: delta` |
| Parquet storage | Implemented and tested | n/a |
| Delta Lake storage | Implemented; needs internet for jars | Databricks runtime has Delta built in |
| Data quality + execution reports | Implemented and tested | Same |
| SQL analytics | Implemented and run locally | Databricks SQL (DDL provided) |
| ADF orchestration | n/a | Template JSON, not deployed |
| Databricks job / notebooks | n/a | Template, not executed |
| Event Hubs streaming | Parser unit-tested | Template, not executed |
| CI (GitHub Actions) | Workflow provided | n/a |
| Azure DevOps CD | n/a | Template, not executed |
