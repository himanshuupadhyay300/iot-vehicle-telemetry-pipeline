#!/usr/bin/env bash
# TEMPLATE - these commands have NOT been executed from this repository.
# Review names/regions, run `az login`, then run step by step. Resources incur Azure charges.
set -euo pipefail

LOCATION="${LOCATION:-centralindia}"
PREFIX="${PREFIX:-vehtel}"                     # lower-case letters/digits, <= 10 chars
RG="rg-${PREFIX}-dev"
STORAGE="st${PREFIX}dev$RANDOM"                # globally unique, 3-24 lower-case alphanumerics
KV="kv-${PREFIX}-dev-$RANDOM"
ADF="adf-${PREFIX}-dev"
DBX="dbx-${PREFIX}-dev"
EHNS="evhns-${PREFIX}-dev"
EH="vehicle-telemetry"

az extension add --name datafactory --only-show-errors
az extension add --name databricks --only-show-errors

echo "1/7 Resource group";   az group create -n "$RG" -l "$LOCATION"

echo "2/7 ADLS Gen2 (hierarchical namespace = true)"
az storage account create -n "$STORAGE" -g "$RG" -l "$LOCATION" \
  --sku Standard_LRS --kind StorageV2 --hns true --min-tls-version TLS1_2 --allow-blob-public-access false
for c in landing lakehouse; do
  az storage container create --account-name "$STORAGE" -n "$c" --auth-mode login
done

echo "3/7 Key Vault (secrets: Databricks token, Event Hubs connection string)"
az keyvault create -n "$KV" -g "$RG" -l "$LOCATION" --enable-rbac-authorization true

echo "4/7 Azure Databricks workspace"
az databricks workspace create -n "$DBX" -g "$RG" -l "$LOCATION" --sku premium

echo "5/7 Azure Data Factory with system-assigned managed identity"
az datafactory create -n "$ADF" -g "$RG" -l "$LOCATION"

echo "6/7 Event Hubs (optional streaming stage)"
az eventhubs namespace create -n "$EHNS" -g "$RG" -l "$LOCATION" --sku Standard
az eventhubs eventhub create -n "$EH" -g "$RG" --namespace-name "$EHNS" --partition-count 2

echo "7/7 Grant ADF managed identity access to the lake"
ADF_PRINCIPAL=$(az datafactory show -n "$ADF" -g "$RG" --query identity.principalId -o tsv)
STORAGE_ID=$(az storage account show -n "$STORAGE" -g "$RG" --query id -o tsv)
az role assignment create --assignee-object-id "$ADF_PRINCIPAL" --assignee-principal-type ServicePrincipal \
  --role "Storage Blob Data Contributor" --scope "$STORAGE_ID"

cat <<MSG

Provisioning commands finished. NEXT (manual / see README):
  * Store secrets:  az keyvault secret set --vault-name $KV --name databricks-access-token --value <token>
  * Create a Databricks secret scope backed by $KV, add the Repos clone, create a cluster.
  * Import pipelines/adf/*.json into $ADF (Studio -> Manage -> ARM/JSON, or az datafactory ... create).
  * Upload telemetry files to  abfss://landing@$STORAGE.dfs.core.windows.net/
MSG
