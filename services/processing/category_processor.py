from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
import logging
from pathlib import Path
from typing import Any

from google.cloud import storage
import pandas as pd

from .detector import CategoryDetectionService
from .frame_prep import DataFramePreparationService
from .naming import FilenameConventionService
from ..upload.jobs import UploadCsvContentParams, upload_csv_content


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class DestinationPathParams:
    category: str
    split_day: date
    output_filename: str


@dataclass
class CategoryProcessingService:
    category_detector: CategoryDetectionService
    naming_service: FilenameConventionService
    storage_client: storage.Client
    target_bucket: str
    processing_configs: dict[str, dict[str, Any]]
    preparation_service: DataFramePreparationService
    _source_bucket: str = field(init=False, default="")
    _object_name: str = field(init=False, default="")
    _input_df: pd.DataFrame | None = field(init=False, default=None)
    _category: str = field(init=False, default="")
    _config: dict[str, Any] = field(init=False, default_factory=dict)

    def process(self, source_bucket: str, object_name: str, input_df: pd.DataFrame) -> list[dict[str, str]]:
        self._source_bucket = source_bucket
        self._object_name = object_name
        self._input_df = input_df

        self._detect_category_and_config()
        normalized_df = self._prepare_category_frame()
        frames_by_date = self._split_category_frame(normalized_df=normalized_df)
        return self._upload_split_outputs(frames_by_date=frames_by_date)

    def _detect_category_and_config(self) -> None:
        if self._input_df is None:
            raise ValueError("Input dataframe is not initialized")

        source_filename = Path(self._object_name).name
        detected = self.category_detector.detect_category(filename=source_filename, df=self._input_df)
        if detected is None:
            raise ValueError(
                "Could not determine category from filename or CSV structure. "
                f"file=gs://{self._source_bucket}/{self._object_name}"
            )

        self._category = detected.category.strip().lower()
        config = self.processing_configs.get(self._category)
        if not isinstance(config, dict):
            raise ValueError(f"No configuration found for detected category: {self._category}")
        self._config = config

        LOGGER.info(
            "Detected category '%s' via %s (confidence=%.3f)",
            self._category,
            detected.source,
            detected.confidence,
        )

    def _prepare_category_frame(self) -> pd.DataFrame:
        if self._input_df is None:
            raise ValueError("Input dataframe is not initialized")

        normalized_df = self.preparation_service.rename_columns(self._input_df, self._config)
        validation_summary = self.preparation_service.validate_patterns(normalized_df, self._config)

        if not validation_summary.is_valid:
            raise ValueError(
                "CSV validation failed after renaming columns. "
                f"category={self._category}, score={validation_summary.score:.3f}, "
                f"threshold={validation_summary.threshold:.3f}, "
                f"failed_rules={validation_summary.failed_rules}/{validation_summary.total_rules}"
            )

        # Keep only configured target fields in upload outputs.
        return self.preparation_service.select_mapped_columns(normalized_df, self._config)

    def _split_category_frame(self, normalized_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
        split_date_column = str(self._config.get("split_date_column", "")).strip()
        if not split_date_column:
            raise ValueError(f"Missing split_date_column for category: {self._category}")

        frames_by_date = self.preparation_service.split_by_date(normalized_df, split_date_column)
        if not frames_by_date:
            raise ValueError(
                f"No valid dated rows available for split_date_column={split_date_column!r}"
            )
        return frames_by_date

    def _upload_split_outputs(self, frames_by_date: dict[date, pd.DataFrame]) -> list[dict[str, str]]:
        uploaded_outputs: list[dict[str, str]] = []
        for split_day, day_frame in frames_by_date.items():
            output_filename = self.naming_service.build_filename(category=self._category, when=split_day)
            destination_path = self._build_destination_path(DestinationPathParams(
                category=self._category,
                split_day=split_day,
                output_filename=output_filename,
            ))
            LOGGER.info(
                "Uploading split output for category=%s date=%s to path=%s",
                self._category,
                split_day.isoformat(),
                destination_path,
            )
            csv_content = day_frame.to_csv(index=False)
            storage_uri = upload_csv_content(UploadCsvContentParams(
                storage_client=self.storage_client,
                target_bucket=self.target_bucket,
                destination_path=destination_path,
                csv_content=csv_content,
            ))
            uploaded_outputs.append(
                {
                    "date": split_day.isoformat(),
                    "filename": output_filename,
                    "destination_path": destination_path,
                    "storage_uri": storage_uri,
                }
            )
            LOGGER.info("Uploaded split output: %s", storage_uri)

        return uploaded_outputs

    def _build_destination_path(self, params: DestinationPathParams) -> str:
        normalized_category = params.category.strip().lower()
        folder_category = self._folder_category_name(normalized_category).strip("/ ")
        year = params.split_day.strftime("%Y")
        month = params.split_day.strftime("%m")
        safe_filename = Path(params.output_filename).name
        return f"{folder_category}/{year}/{month}/{safe_filename}"

    @staticmethod
    def _folder_category_name(category: str) -> str:
        # Enforce lowercase folder names for all categories.
        category_folder_map = {
            "contact": "contact",
            "ev": "ev",
            "print": "print",
        }
        return category_folder_map.get(category, category).lower()
