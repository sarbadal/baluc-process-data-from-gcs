# Codebase Flowchart

This document visualizes the main execution paths in the project.

## 1) Runtime Data Pipeline (GCS Trigger -> Process -> Upload)

```mermaid
flowchart TD
    A[GCS Object Finalize Event] --> B[main.py: process_gcs_file]
    B --> C{Bucket matches SOURCE_BUCKET?}
    C -- No --> C1[Ignore event]
    C -- Yes --> D{Object is .csv?}
    D -- No --> D1[Ignore non-CSV]
    D -- Yes --> E[_get_processor]

    E --> F{Processor already cached?}
    F -- Yes --> G[Reuse FileProcessingService]
    F -- No --> H[_build_processor]

    H --> I[Read env vars]
    I --> J[build_google_auth_context]
    J --> K{Key file exists?}
    K -- Yes --> K1[Service account credentials]
    K -- No --> K2[ADC fallback]
    K1 --> L[create_storage_client]
    K2 --> L

    H --> M[load_processing_configs from file_config/*.json]
    H --> N[FilenameConventionService(default_naming_rules)]
    L --> O[Create FileProcessingService]
    M --> O
    N --> O
    O --> G

    G --> P[process_uploaded_object]
    P --> Q[_download_csv from source bucket]
    Q --> R{CSV empty?}
    R -- Yes --> R1[Raise error]
    R -- No --> S[CategoryDetectionService.detect_category]

    S --> T{Category detected?}
    T -- No --> T1[Raise error]
    T -- Yes --> U[_rename_columns using field_mapping]
    U --> V[_validate_patterns]
    V --> W{Validation score >= threshold?}
    W -- No --> W1[Raise error]
    W -- Yes --> X[_split_by_date using split_date_column]

    X --> Y{Any dated groups?}
    Y -- No --> Y1[Raise error]
    Y -- Yes --> Z[For each split date]

    Z --> AA[naming_service.build_filename]
    AA --> AB[_build_destination_path category/YYYY/MM/file]
    AB --> AC[upload_csv_content to target bucket]
    AC --> AD[Collect output metadata]
    AD --> AE[Return uploaded outputs]
```

## 2) Category Detection Logic

```mermaid
flowchart TD
    A[detect_category filename, df] --> B[Try filename pattern match]
    B --> C{Matched?}
    C -- Yes --> D[Return category source=filename confidence=1.0]
    C -- No --> E{DataFrame available?}
    E -- No --> F[Return None]
    E -- Yes --> G[Score each category config]

    G --> H[Header score from source/target column overlap]
    G --> I[Optional content hint score from regex patterns]
    H --> J[Combine weighted score]
    I --> J
    J --> K[Small bonus if split_date_column exists]

    K --> L[Rank categories]
    L --> M{Top score >= min_confidence?}
    M -- No --> N[Return None]
    M -- Yes --> O{Tie within tie_break_delta?}
    O -- Yes --> P[Return None ambiguous]
    O -- No --> Q[Return top category source=csv_fields]
```

## 3) Upload Job Manager Path (Async file uploads)

```mermaid
flowchart TD
    A[start_job files/categories/file_types] --> B[_materialize_uploads to temp files]
    B --> C[Create job state queued]
    C --> D[Start worker thread _run_job]

    D --> E[Set status running]
    E --> F[Loop each prepared file]
    F --> G{Cancel requested?}
    G -- Yes --> H[Raise UploadJobCanceled]
    G -- No --> I[Build UploadRequest]
    I --> J[upload_service.handle_upload]

    J --> K[progress_callback -> _on_progress]
    K --> K1[frames_ready or uploading_output or uploaded_output]
    K1 --> K2[_upsert_active_result + _set_job_state]

    J --> L[_append_result]
    L --> M[processed_files += 1]
    M --> N{More files?}
    N -- Yes --> F
    N -- No --> O[Set status completed]

    H --> P[Set status canceled]
    J --> Q{Exception?}
    Q -- Yes --> R[Set status failed]

    O --> S[Cleanup temp files]
    P --> S
    R --> S
```

## 4) Deployment Script Path

```mermaid
flowchart TD
    A[python deployment.py --env-file ...] --> B[parse_env_file]
    B --> C[validate_required_env]
    C --> D[build_deploy_context]
    D --> E[resolve_trigger_location]
    E --> F{TRIGGER_LOCATION provided?}
    F -- Yes --> G[Use configured location]
    F -- No --> H[detect_bucket_location via gcloud]
    H --> I{Detected?}
    I -- No --> J[Raise error]
    I -- Yes --> G

    D --> K{--auto-grant-pubsub-publisher?}
    K -- Yes --> L[Grant roles/pubsub.publisher]
    K -- No --> M[Skip auto-grant]

    G --> N[build_gcloud_command]
    L --> N
    M --> N
    N --> O{--dry-run?}
    O -- Yes --> P[Print command only]
    O -- No --> Q[run_deploy]
```

## 5) Key File Relationships

```mermaid
flowchart LR
    main_py[main.py] --> file_processor[services/file_processor.py]
    main_py --> google_auth[services/google_auth.py]
    main_py --> naming[services/naming.py]

    file_processor --> category_detection[services/category_detection.py]
    file_processor --> upload_jobs[services/upload_jobs.py]
    file_processor --> naming

    upload_jobs --> upload_service[services/upload_service.py]

    deployment_py[deployment.py] --> env_file[deploy.env / deploy.env.example]
    file_processor --> config_files[file_config/contact.json ev.json print.json]
```
