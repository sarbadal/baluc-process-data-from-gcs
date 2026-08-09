# BaluC - Process Data from GCS

Google Cloud Function to process CSV files uploaded to a source Google Cloud Storage bucket, detect category, normalize and validate columns, split by date, and upload daily outputs to a target bucket.

## Quick Start (2 min)

1. Install dependencies.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Prepare environment files.

```bash
cp .env.example .env
cp deploy.env.example deploy.env
```

3. Set values in deploy.env:

- GOOGLE_CLOUD_PROJECT_ID
- SOURCE_BUCKET
- TARGET_BUCKET
- Optional: GOOGLE_AUTH_KEY_PATH, VALIDATION_MIN_SCORE, TRIGGER_LOCATION

Sample `deploy.env`:

```env
GOOGLE_CLOUD_PROJECT_ID=your-gcp-project-id
GOOGLE_AUTH_KEY_PATH=_google_auth_key.json
SOURCE_BUCKET=your-source-bucket
TRIGGER_LOCATION=us-central1
TARGET_BUCKET=your-target-bucket
VALIDATION_MIN_SCORE=0.7
```

Set TRIGGER_LOCATION to the SOURCE_BUCKET location (for example `us` for multi-region US buckets).

4. Deploy.

```bash
python deployment.py --env-file deploy.env
```

If you hit Eventarc trigger permission error for Pub/Sub publisher role:

```bash
python deployment.py --env-file deploy.env --auto-grant-pubsub-publisher
```

Optional dry run:

```bash
python deployment.py --env-file deploy.env --dry-run
```

## What This Project Does

1. Trigger on new CSV file upload in source bucket.
2. Detect category using existing category detection service.
3. Load category config from file_config.
4. Rename columns using config field mapping.
5. Validate configured column/value patterns with weights.
6. Split into one CSV per date from split_date_column.
7. Generate output file names using existing naming service.
8. Upload daily files to target bucket using category/year/month path.

## Supported Categories

- contact
- ev (uploaded into EV folder)
- print

## Target Object Path

Files are uploaded to:

TARGET_BUCKET/<category>/<YYYY>/<MM>/<file_name>.csv

Examples:

- contact/2026/08/contact_fact_20260809.csv
- EV/2026/08/ev_fact_20260809.csv
- print/2026/08/print_fact_20260809.csv

## Authentication Strategy

Authentication is centralized in services/google_auth.py.

Priority:

1. If GOOGLE_AUTH_KEY_PATH exists, load service-account credentials from that file.
2. Otherwise, explicitly fall back to Google Application Default Credentials (ADC).

Project selection is explicit via GOOGLE_CLOUD_PROJECT_ID.

No private key contents are logged.

## Configuration

Use .env locally (see .env.example), or set env vars in Cloud Functions.

Required:

- GOOGLE_CLOUD_PROJECT_ID
- SOURCE_BUCKET
- TARGET_BUCKET

Optional:

- GOOGLE_AUTH_KEY_PATH (default: _google_auth_key.json)
- VALIDATION_MIN_SCORE (default: 0.7)

## Local Setup

1. Create virtual environment and install dependencies.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Create local env file.

```bash
cp .env.example .env
```

3. Run locally with Functions Framework.

```bash
export $(grep -v '^#' .env | xargs)
functions-framework --target=process_gcs_file --signature-type=event --port=8080
```

## Deploy

Preferred scripted deploy:

1. Prepare deploy env file.

```bash
cp deploy.env.example deploy.env
```

2. Deploy.

```bash
python deployment.py --env-file deploy.env
```

Optional:

```bash
python deployment.py --env-file deploy.env --dry-run
```

## Cloud Function Entry Point

- Function: process_gcs_file
- File: main.py

## Key Files

- main.py: Cloud Function entry point and processor initialization.
- services/file_processor.py: Core file processing flow.
- services/category_detection.py: Category detection logic.
- services/naming.py: Output filename conventions.
- services/upload_jobs.py: Upload helper used by processor.
- services/google_auth.py: Centralized Google Cloud authentication.
- deployment.py: Scripted deployment tool.
- DEPLOYMENT.md: Additional deployment documentation.

## Security Notes

- _google_auth_key.json is ignored by git (see .gitignore).
- Do not commit service account keys.
- In Cloud deployment, key file is not required when ADC is available through attached service account.
