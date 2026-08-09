# Google Cloud Function Deployment

## Entry Point
- Function name: `process_gcs_file`
- Source file: `main.py`

## Required Environment Variables
- `GOOGLE_CLOUD_PROJECT_ID`: Explicit GCP project used for Google Cloud clients.
- `SOURCE_BUCKET`: Bucket that receives incoming CSV files.
- `TARGET_BUCKET`: Bucket where split CSV files are uploaded.

Optional:
- `GOOGLE_AUTH_KEY_PATH`: Path to service-account key JSON (default `_google_auth_key.json`).
- `VALIDATION_MIN_SCORE`: Weighted validation threshold for configured column value patterns (default `0.7`).

## Authentication Priority
1. If `GOOGLE_AUTH_KEY_PATH` exists, clients use that service-account key explicitly.
2. Otherwise, clients fall back to Application Default Credentials (ADC).

This supports local development with a key file and Cloud Functions deployment without bundling the key.

## Deploy (2nd Gen Cloud Functions)

### Option A: Scripted Deploy

Create a deploy env file (for example `deploy.env`) from `deploy.env.example`, then run:

```bash
python deployment.py --env-file deploy.env
```

Optional dry run:

```bash
python deployment.py --env-file deploy.env --dry-run
```

### Option B: Direct gcloud Command

```bash
gcloud functions deploy process-gcs-file \
  --gen2 \
  --runtime=python312 \
  --region=us-central1 \
  --source=. \
  --entry-point=process_gcs_file \
  --trigger-bucket=${SOURCE_BUCKET} \
  --set-env-vars=GOOGLE_CLOUD_PROJECT_ID=${GOOGLE_CLOUD_PROJECT_ID},SOURCE_BUCKET=${SOURCE_BUCKET},TARGET_BUCKET=${TARGET_BUCKET},VALIDATION_MIN_SCORE=0.7
```

## Local Run (Functions Framework)

```bash
export $(grep -v '^#' .env | xargs)
functions-framework --target=process_gcs_file --signature-type=event --port=8080
```

Event payload fields expected from GCS finalize trigger:
- `bucket`
- `name`
