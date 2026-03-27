# Azure ML Weekly Retrain Scaffold

This folder contains a practical Azure ML control-plane scaffold for the hybrid setup:

- Databricks keeps raw reads, feature aggregation, daily scoring, and write-back to Unity Catalog.
- Azure ML consumes curated training snapshots, retrains models, evaluates promotion gates, and updates the production manifest only when the candidate is good enough.

## Files

- `components/validate_snapshot.py`
  - Validates a `training_snapshot` manifest and checks that required exported assets exist.
- `components/train_from_snapshot.py`
  - Loads the curated labeled feature table from a snapshot, retrains the sklearn model, and packages an immutable artifact bundle.
- `components/promote_model.py`
  - Applies promotion gates and writes an approved production manifest when the candidate passes.
- `pipeline_weekly_retrain.py`
  - Azure ML pipeline scaffold that wires the three steps together using Azure ML command jobs.

## Recommended operating model

1. Databricks weekly job writes a `training_snapshot` to Blob or a mounted path.
   - If using SAS-based upload, push the training set files into `training-set/` and register or refresh your Azure ML data asset from that folder.
2. Azure ML weekly pipeline runs:
   1. validate snapshot
   2. train from snapshot
   3. promote candidate if metrics pass
3. Databricks daily scoring job runs with:
   - `--skip-train`
   - `--model-source artifact_bundle`
   - `--model-release-manifest <production_manifest.json>`

## Minimal local examples

Validate a snapshot:

```bash
python3 azureml/components/validate_snapshot.py \
  --snapshot-root /path/to/training-set \
  --output-json /tmp/validate_snapshot_result.json
```

Train and package a candidate bundle:

```bash
python3 azureml/components/train_from_snapshot.py \
  --snapshot-root /path/to/training-set \
  --artifact-dir /tmp/brand-health-artifacts \
  --model-bundle-root /tmp/model-registry \
  --candidate-manifest-out /tmp/candidate_manifest.json \
  --output-json /tmp/train_result.json
```

Promote a candidate:

```bash
python3 azureml/components/promote_model.py \
  --candidate-manifest /tmp/model-registry/latest_candidate.json \
  --production-manifest-out /tmp/model-registry/production_manifest.json \
  --output-json /tmp/promote_result.json
```

## Azure ML notes

- The pipeline script intentionally keeps Azure-specific dependencies isolated to `pipeline_weekly_retrain.py`.
- Component scripts use repo-local code and can run both locally and inside Azure ML command jobs.
- Use a mounted Blob/ADLS path or an Azure ML data asset path that resolves to a local filesystem path inside the job.

Example Azure ML pipeline submission with explicit workspace identifiers:

```bash
python3 azureml/pipeline_weekly_retrain.py \
  --subscription-id <sub_id> \
  --resource-group <rg> \
  --workspace-name <aml_ws> \
  --compute <aml_compute> \
  --environment <aml_env_name:version> \
  --training-set-root azureml:<your_training_set_data_asset>:<version> \
  --model-bundle-root <shared-path>/model_registry \
  --production-manifest-out <shared-path>/model_registry/production_manifest.json
```

Example Azure ML pipeline submission from an Azure ML compute or workspace repo that already has `config.json`:

```bash
python3 azureml/pipeline_weekly_retrain.py \
  --config-path . \
  --compute cpu-cluster \
  --environment brand-health-train:3 \
  --training-set-root azureml:brand-health-training-set:20260327 \
  --model-bundle-root azureml://datastores/workspaceblobstore/paths/brand-health/model_registry/ \
  --production-manifest-out azureml://datastores/workspaceblobstore/paths/brand-health/model_registry/production_manifest.json
```
