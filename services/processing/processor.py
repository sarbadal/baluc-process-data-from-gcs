from __future__ import annotations

from dataclasses import dataclass, field
import io
import json
import logging
from pathlib import Path
from typing import Any

from google.cloud import storage
import pandas as pd

from .category_processor import CategoryProcessingService
from .detector import CategoryDetectionService
from .frame_prep import DataFramePreparationService
from .mapping_router import MappingRouter
from .naming import FilenameConventionService


LOGGER = logging.getLogger(__name__)


@dataclass
class FileProcessingService:
    storage_client: storage.Client
    target_bucket: str
    naming_service: FilenameConventionService
    processing_configs: dict[str, dict[str, Any]]
    routing_configs: dict[str, dict[str, Any]] = field(default_factory=dict)
    validation_min_score: float = 0.7
    _category_detector: CategoryDetectionService = field(init=False)
    _preparation_service: DataFramePreparationService = field(init=False)
    _mapping_router: MappingRouter = field(init=False)
    _category_processing_service: CategoryProcessingService = field(init=False)

    def __post_init__(self) -> None:
        self._category_detector = CategoryDetectionService(
            naming_service=self.naming_service,
            processing_category_configs=self.processing_configs,
        )
        self._preparation_service = DataFramePreparationService(
            validation_min_score=self.validation_min_score,
        )
        self._mapping_router = MappingRouter(
            storage_client=self.storage_client,
            target_bucket=self.target_bucket,
            routing_configs=self.routing_configs,
            preparation_service=self._preparation_service,
        )
        self._category_processing_service = CategoryProcessingService(
            category_detector=self._category_detector,
            naming_service=self.naming_service,
            storage_client=self.storage_client,
            target_bucket=self.target_bucket,
            processing_configs=self.processing_configs,
            preparation_service=self._preparation_service,
        )

    def process_uploaded_object(self, source_bucket: str, object_name: str) -> list[dict[str, str]]:
        LOGGER.info("Starting processing for gs://%s/%s", source_bucket, object_name)

        input_df = self._download_csv(source_bucket=source_bucket, object_name=object_name)
        if input_df.empty:
            raise ValueError(f"Input CSV is empty: gs://{source_bucket}/{object_name}")

        routed_output = self._mapping_router.try_route_configured_file(
            source_bucket=source_bucket,
            object_name=object_name,
            input_df=input_df,
        )
        if routed_output is not None:
            return [routed_output]

        uploaded_outputs = self._category_processing_service.process(
            source_bucket=source_bucket,
            object_name=object_name,
            input_df=input_df,
        )

        LOGGER.info(
            "Processing completed for gs://%s/%s. Outputs=%d",
            source_bucket,
            object_name,
            len(uploaded_outputs),
        )
        return uploaded_outputs

    def _download_csv(self, source_bucket: str, object_name: str) -> pd.DataFrame:
        blob = self.storage_client.bucket(source_bucket).blob(object_name)
        csv_bytes = blob.download_as_bytes()
        return pd.read_csv(io.BytesIO(csv_bytes))


def load_processing_configs(config_dir: str | Path) -> dict[str, dict[str, Any]]:
    base_dir = Path(config_dir)
    category_files = {
        "contact": "contact.json",
        "ev": "ev.json",
        "print": "print.json",
    }

    configs: dict[str, dict[str, Any]] = {}
    for category, filename in category_files.items():
        config_path = base_dir / filename
        with config_path.open("r", encoding="utf-8") as config_file:
            raw = json.load(config_file)

        if not isinstance(raw, dict):
            raise ValueError(f"Invalid config structure in {config_path}")

        configs[category] = raw

    return configs


def load_routing_configs(config_dir: str | Path) -> dict[str, dict[str, Any]]:
    base_dir = Path(config_dir)
    configs: dict[str, dict[str, Any]] = {}

    for config_path in sorted(base_dir.glob("*.json")):
        with config_path.open("r", encoding="utf-8") as config_file:
            raw = json.load(config_file)

        if not isinstance(raw, dict):
            continue

        gcs_path = str(raw.get("gcs_path", "")).strip()
        if not gcs_path:
            continue

        config_name = config_path.stem.strip().lower()
        if not config_name:
            continue

        configs[config_name] = raw

    return configs


def default_naming_rules() -> dict[str, dict[str, Any]]:
    return {
        "contact": {
            "match_patterns": [r".*contact.*\.csv$"],
            "output_stem": "contact_fact",
            "default_file_type": "fact",
        },
        "ev": {
            "match_patterns": [r".*ev.*\.csv$", r".*electric.*\.csv$"],
            "output_stem": "ev_fact",
            "default_file_type": "fact",
        },
        "print": {
            "match_patterns": [r".*print.*\.csv$"],
            "output_stem": "print_fact",
            "default_file_type": "fact",
        },
    }
