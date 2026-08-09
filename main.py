from __future__ import annotations

import logging
import os
from pathlib import Path

from dotenv import load_dotenv

from services.file_processor import (
    FileProcessingService,
    default_naming_rules,
    load_processing_configs,
)
from services.google_auth import build_google_auth_context, create_storage_client
from services.naming import FilenameConventionService


logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

load_dotenv()

_PROCESSOR: FileProcessingService | None = None


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _build_processor() -> FileProcessingService:
    project_id = _require_env("GOOGLE_CLOUD_PROJECT_ID")
    google_auth_key_path = os.getenv("GOOGLE_AUTH_KEY_PATH", "_google_auth_key.json").strip()
    if not google_auth_key_path:
        google_auth_key_path = "_google_auth_key.json"

    target_bucket = _require_env("TARGET_BUCKET")

    validation_threshold_raw = os.getenv("VALIDATION_MIN_SCORE", "0.7").strip()
    try:
        validation_threshold = float(validation_threshold_raw)
    except ValueError as err:
        raise RuntimeError("VALIDATION_MIN_SCORE must be a float") from err

    project_root = Path(__file__).resolve().parent
    config_dir = project_root / "file_config"

    auth_context = build_google_auth_context(
        project_id=project_id,
        configured_key_path=google_auth_key_path,
        base_dir=project_root,
    )

    configs = load_processing_configs(config_dir)
    naming_service = FilenameConventionService(default_naming_rules())

    return FileProcessingService(
        storage_client=create_storage_client(auth_context),
        target_bucket=target_bucket,
        naming_service=naming_service,
        processing_configs=configs,
        validation_min_score=validation_threshold,
    )


def _get_processor() -> FileProcessingService:
    global _PROCESSOR
    if _PROCESSOR is None:
        _PROCESSOR = _build_processor()
    return _PROCESSOR


def process_gcs_file(event: dict, _context: object) -> None:
    """Cloud Function entry point for GCS object finalize events."""

    source_bucket = _require_env("SOURCE_BUCKET")

    event_bucket = str(event.get("bucket", "")).strip()
    object_name = str(event.get("name", "")).strip()

    if not event_bucket or not object_name:
        LOGGER.warning("Skipping event with missing bucket/name: %s", event)
        return

    if event_bucket != source_bucket:
        LOGGER.info(
            "Ignoring object from unexpected bucket. expected=%s, received=%s, object=%s",
            source_bucket,
            event_bucket,
            object_name,
        )
        return

    if not object_name.lower().endswith(".csv"):
        LOGGER.info("Ignoring non-CSV object: gs://%s/%s", event_bucket, object_name)
        return

    processor = _get_processor()
    outputs = processor.process_uploaded_object(source_bucket=event_bucket, object_name=object_name)

    LOGGER.info(
        "Generated %d split file(s) for gs://%s/%s",
        len(outputs),
        event_bucket,
        object_name,
    )
