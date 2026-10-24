# Design decisions, trade-offs and failure modes

Written for code review and interviews: *why* it is built this way, what it costs, and where it can break.

## 1. Key identity and de-duplication

**Decision:** an event is identified by `(vehicle_id, event_ts)`. The latest *ingested* copy wins; ties break on
`_source_file`, then `_row_id`.

- Trade-off: a vehicle that legitimately emits two different readings with the same timestamp loses one. If the
  real source had a sequence number or message id, that would be a better key. The key is a single constant
  (`KEYS` in `silver.py`).
- The pipeline counts *conflicting* duplicates (same key, different payload) separately from exact duplicates,
  because conflicts hint at a device or upstream bug that exact duplicates do not.

## 2. Late-arriving data

**Decision:** a late event is a *new* key whose `event_ts` is older than that vehicle's current Silver maximum.
It is accepted (never dropped), flagged `is_late_arrival`, and Gold is recomputed for its date.

- Why not a streaming watermark that discards old data? For telemetry used in engineering analysis, dropping
  data silently is worse than recomputing a day. The cost is recomputation of whole days (cheap at daily grain).
- Verified behaviour: in the sample, 15 withheld readings arrive in batch 2; VH-0001's day-1 gap segment
  disappears and its distance increases (asserted in `test_late_data_corrects_gold`).
- Known limit: a "late" event for a vehicle that has never been seen cannot be detected (no watermark yet).

## 3. Idempotency (three layers)

1. **File level:** SHA-256 checksum in the state store. A renamed copy is skipped.
2. **Merge level:** Silver upsert on the business key, and an older ingestion never overwrites a newer one.
3. **Output level:** Gold is rebuilt per `event_date` partition (replace, not append); quarantine rows are replaced
   per `_batch_id`.

Failure mode covered: process dies after the Bronze write but before the state update. The file is re-ingested
(Bronze gets duplicate rows) but Silver de-duplicates, so analytics stay correct. Bronze is an audit log and is
allowed to contain duplicates by design.

## 4. Quarantine instead of fail-fast

Invalid rows go to `silver_rejects` with the list of broken rules; the run still succeeds. Rationale: one bad
sensor should not block the whole fleet. Trade-off: someone must watch the rejection rate. `DataQualityError`
exists for a future "fail the run if rejects exceed X%" gate (not implemented).

Required vs optional fields: a missing speed or GPS coordinate makes a record unusable for distance/usage
metrics, so it is rejected. A missing fuel or voltage reading is kept with a warning because the other metrics
are still valid.

## 5. Parquet locally, Delta on Azure

The `storage.py` module implements the same four operations (append, replace partitions, upsert, overwrite) on
both formats.

- Parquet gives a zero-download local run and fast CI.
- The Parquet upsert is **not atomic**: it reads affected partitions, merges in memory, and overwrites them. A
  crash mid-overwrite can leave a partition missing until the next run recreates it. Delta's `MERGE` and
  `replaceWhere` are transactional, which is the reason Delta is the production target.
- Honest status: the Delta path is implemented and has a dedicated test, but it could not be executed in the
  environment where this project was built (Delta jars are fetched from Maven). It runs in a non-blocking CI job.

## 6. Distance and usage calculation

Distance is the sum of haversine segments between consecutive readings, but a segment is counted only if the gap
is at most `max_gap_minutes` and the implied speed is plausible. Otherwise it is reported as a `data_gap` or
`gps_jump` segment rather than silently inflating distance.

- Simplifications: segments do not cross midnight (UTC), and straight-line segments underestimate true path
  length on curved roads at one-minute sampling.

## 7. State store as a JSON file

Simple, inspectable, atomic via rename. It requires a path Python can open, which on Databricks means a Unity
Catalog Volume or DBFS FUSE path. At scale, or with concurrent writers, replace it with a Delta control table.
It is **not** safe for two pipeline runs at the same time (no locking).

## 8. Performance notes (design, not measurements)

- No benchmark numbers are claimed. The sample is ~1,200 rows; runtime is dominated by Spark start-up.
- Silver is partitioned by `event_date`; upserts touch only affected partitions.
- Several `count()` actions on cached DataFrames exist to produce data quality metrics. On very large batches
  these metrics could be computed in a single aggregation pass.
- Gold daily recompute uses window functions per (vehicle, day); skew appears if one vehicle dominates.
- Delta: run `OPTIMIZE ... ZORDER BY (vehicle_id, event_ts)` periodically (see `sql/databricks_create_tables.sql`).

## 9. Likely interview questions

**How do you handle duplicates and late data?**
Business-key upsert with latest-ingested-wins, late events flagged and accepted, Gold partitions recomputed for the
affected dates.

**What if the job fails halfway?**
Stage status is persisted per batch; the next run resumes pending stages. Every stage is idempotent, so re-running is safe.

**Why quarantine rather than drop?**
Auditability and replay: rejected rows keep their raw values and the exact rules they broke.

**Why Medallion?**
Bronze preserves truth, Silver is the single cleaned source, Gold serves specific consumers. Fixing a rule means
re-deriving Silver/Gold from Bronze without re-collecting data.

**What would you change for 10,000 vehicles at 1 Hz?**
Auto Loader or Event Hubs with structured streaming into Bronze, Delta `MERGE` with partition pruning on
`event_date`, Z-ORDER or liquid clustering, a Delta control table for state, and metrics computed in one pass.
