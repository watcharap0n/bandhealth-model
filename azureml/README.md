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
  --snapshot-manifest /path/to/training_snapshot/snapshot_manifest.json \
  --output-json /tmp/validate_snapshot_result.json
```

Train and package a candidate bundle:

```bash
python3 azureml/components/train_from_snapshot.py \
  --snapshot-manifest /path/to/training_snapshot/snapshot_manifest.json \
  --artifact-dir /tmp/brand-health-artifacts \
  --model-bundle-root /tmp/model-registry \
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
