from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from google.cloud import storage
import pandas as pd

from .frame_prep import DataFramePreparationService
from ..upload.jobs import UploadCsvContentParams, upload_csv_content


@dataclass
class MappingRouter:
    storage_client: storage.Client
    target_bucket: str
    routing_configs: dict[str, dict[str, Any]]
    preparation_service: DataFramePreparationService
    _object_name: str = field(init=False, default="")
    _input_df: pd.DataFrame | None = field(init=False, default=None)

    def try_route_configured_file(self, source_bucket: str, object_name: str, input_df: pd.DataFrame) -> dict[str, str] | None:
        if not self.routing_configs:
            return None

        self._object_name = object_name
        self._input_df = input_df

        for config_name, config in self.routing_configs.items():
            if not self._matches_routing_config(config=config):
                continue

            raw_gcs_path = str(config.get("gcs_path", "")).strip()
            destination_path = self._build_mapping_destination_path(raw_gcs_path=raw_gcs_path)

            normalized_df = self.preparation_service.rename_columns(self._input_df, config)
            selected_df = self.preparation_service.select_mapped_columns(normalized_df, config)
            csv_content = selected_df.to_csv(index=False)
            storage_uri = upload_csv_content(UploadCsvContentParams(
                storage_client=self.storage_client,
                target_bucket=self.target_bucket,
                destination_path=destination_path,
                csv_content=csv_content,
            ))

            return {
                "date": "",
                "filename": Path(destination_path).name,
                "destination_path": destination_path,
                "storage_uri": storage_uri,
            }

        return None

    def _matches_routing_config(self, config: dict[str, Any]) -> bool:
        if self._input_df is None:
            raise ValueError("Input dataframe is not initialized")

        field_mapping = config.get("field_mapping")
        if not isinstance(field_mapping, dict) or not field_mapping:
            return False

        required_source_columns = {
            str(source).strip().lower()
            for source in field_mapping.values()
            if isinstance(source, str) and source.strip()
        }
        available_columns = {str(column).strip().lower() for column in self._input_df.columns}
        if required_source_columns and not required_source_columns.issubset(available_columns):
            return False

        hints = config.get("content_hints")
        pattern_rules = None
        if isinstance(hints, dict):
            pattern_rules = hints.get("column_value_patterns")
            if pattern_rules is None:
                pattern_rules = hints.get("column_value_pattern")
        if not isinstance(pattern_rules, list) or not pattern_rules:
            return False

        try:
            normalized_df = self.preparation_service.rename_columns(self._input_df, config)
        except ValueError:
            return False

        validation = self.preparation_service.validate_patterns(normalized_df, config)
        return validation.is_valid and validation.failed_rules == 0 and validation.total_rules > 0

    def _build_mapping_destination_path(self, raw_gcs_path: str) -> str:
        # Mapping outputs are always written under mapping/ in target bucket.
        filename = Path(self._object_name).name
        if raw_gcs_path:
            _, object_path = self.preparation_service.parse_gcs_path(raw_gcs_path)
            configured_name = Path(object_path).name
            if configured_name:
                filename = configured_name

        return f"mapping/{filename}"
