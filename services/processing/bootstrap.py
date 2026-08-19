from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

from services.auth.gauth import build_google_auth_context, create_storage_client
from services.processing.processor import (
    FileProcessingService,
    default_naming_rules,
    load_processing_configs,
    load_routing_configs,
)
from services.processing.naming import FilenameConventionService


load_dotenv()

_PROCESSOR: FileProcessingService | None = None


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def get_source_bucket() -> str:
    return require_env("SOURCE_BUCKET")


def _validation_threshold() -> float:
    validation_threshold_raw = os.getenv("VALIDATION_MIN_SCORE", "0.7").strip()
    try:
        return float(validation_threshold_raw)
    except ValueError as err:
        raise RuntimeError("VALIDATION_MIN_SCORE must be a float") from err


def _google_auth_key_path() -> str:
    configured = os.getenv("GOOGLE_AUTH_KEY_PATH", "_google_auth_key.json").strip()
    return configured or "_google_auth_key.json"


def _build_processor(project_root: Path) -> FileProcessingService:
    project_id = require_env("GOOGLE_CLOUD_PROJECT_ID")
    target_bucket = require_env("TARGET_BUCKET")
    auth_key_path = _google_auth_key_path()

    config_dir = project_root / "file_config"
    auth_context = build_google_auth_context(
        project_id=project_id,
        configured_key_path=auth_key_path,
        base_dir=project_root,
    )

    configs = load_processing_configs(config_dir)
    routing_configs = load_routing_configs(config_dir)
    naming_service = FilenameConventionService(default_naming_rules())

    return FileProcessingService(
        storage_client=create_storage_client(auth_context),
        target_bucket=target_bucket,
        naming_service=naming_service,
        processing_configs=configs,
        routing_configs=routing_configs,
        validation_min_score=_validation_threshold(),
    )


def get_processor(project_root: Path) -> FileProcessingService:
    global _PROCESSOR
    if _PROCESSOR is None:
        _PROCESSOR = _build_processor(project_root)
    return _PROCESSOR
