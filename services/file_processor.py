from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
import io
import json
import logging
from pathlib import Path
import re
from typing import Any

from google.cloud import storage
import pandas as pd

from .category_detection import CategoryDetectionService
from .naming import FilenameConventionService
from .upload_jobs import UploadCsvContentParams, upload_csv_content


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class ValidationSummary:
    is_valid: bool
    score: float
    threshold: float
    total_rules: int
    failed_rules: int


@dataclass(frozen=True)
class DestinationPathParams:
    category: str
    split_day: date
    output_filename: str


@dataclass
class FileProcessingService:
    storage_client: storage.Client
    target_bucket: str
    naming_service: FilenameConventionService
    processing_configs: dict[str, dict[str, Any]]
    validation_min_score: float = 0.7
    _category_detector: CategoryDetectionService = field(init=False)

    def __post_init__(self) -> None:
        self._category_detector = CategoryDetectionService(
            naming_service=self.naming_service,
            processing_category_configs=self.processing_configs,
        )

    def process_uploaded_object(self, source_bucket: str, object_name: str) -> list[dict[str, str]]:
        LOGGER.info("Starting processing for gs://%s/%s", source_bucket, object_name)

        input_df = self._download_csv(source_bucket=source_bucket, object_name=object_name)
        if input_df.empty:
            raise ValueError(f"Input CSV is empty: gs://{source_bucket}/{object_name}")

        source_filename = Path(object_name).name
        detected = self._category_detector.detect_category(filename=source_filename, df=input_df)
        if detected is None:
            raise ValueError(
                "Could not determine category from filename or CSV structure. "
                f"file=gs://{source_bucket}/{object_name}"
            )

        category = detected.category.strip().lower()
        config = self.processing_configs.get(category)
        if not isinstance(config, dict):
            raise ValueError(f"No configuration found for detected category: {category}")

        LOGGER.info(
            "Detected category '%s' via %s (confidence=%.3f)",
            category,
            detected.source,
            detected.confidence,
        )

        normalized_df = self._rename_columns(input_df, config)
        validation_summary = self._validate_patterns(normalized_df, config)

        if not validation_summary.is_valid:
            raise ValueError(
                "CSV validation failed after renaming columns. "
                f"category={category}, score={validation_summary.score:.3f}, "
                f"threshold={validation_summary.threshold:.3f}, "
                f"failed_rules={validation_summary.failed_rules}/{validation_summary.total_rules}"
            )

        split_date_column = str(config.get("split_date_column", "")).strip()
        if not split_date_column:
            raise ValueError(f"Missing split_date_column for category: {category}")

        frames_by_date = self._split_by_date(normalized_df, split_date_column)
        if not frames_by_date:
            raise ValueError(
                f"No valid dated rows available for split_date_column={split_date_column!r}"
            )

        uploaded_outputs: list[dict[str, str]] = []
        for split_day, day_frame in frames_by_date.items():
            output_filename = self.naming_service.build_filename(category=category, when=split_day)
            destination_path = self._build_destination_path(DestinationPathParams(
                category=category,
                split_day=split_day,
                output_filename=output_filename,
            ))
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

        LOGGER.info(
            "Processing completed for gs://%s/%s. Outputs=%d",
            source_bucket,
            object_name,
            len(uploaded_outputs),
        )
        return uploaded_outputs

    def _build_destination_path(self, params: DestinationPathParams) -> str:
        folder_category = self._folder_category_name(params.category)
        year = params.split_day.strftime("%Y")
        month = params.split_day.strftime("%m")
        return f"{folder_category}/{year}/{month}/{params.output_filename}"

    @staticmethod
    def _folder_category_name(category: str) -> str:
        # Preserve existing category behavior while matching required target path casing.
        category_folder_map = {
            "contact": "contact",
            "ev": "EV",
            "print": "print",
        }
        return category_folder_map.get(category, category)

    def _download_csv(self, source_bucket: str, object_name: str) -> pd.DataFrame:
        blob = self.storage_client.bucket(source_bucket).blob(object_name)
        csv_bytes = blob.download_as_bytes()
        return pd.read_csv(io.BytesIO(csv_bytes))

    def _rename_columns(self, input_df: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
        field_mapping = config.get("field_mapping")
        if not isinstance(field_mapping, dict) or not field_mapping:
            raise ValueError("Invalid or empty field_mapping in category config")

        normalized_to_actual = {str(col).strip().lower(): col for col in input_df.columns}
        rename_map: dict[str, str] = {}

        for target_name, source_name in field_mapping.items():
            if not isinstance(target_name, str) or not target_name.strip():
                continue
            if not isinstance(source_name, str) or not source_name.strip():
                continue

            source_actual = normalized_to_actual.get(source_name.strip().lower())
            if source_actual is not None:
                rename_map[source_actual] = target_name

        renamed_df = input_df.rename(columns=rename_map)

        expected_targets_ordered = [
            str(target).strip()
            for target in field_mapping.keys()
            if isinstance(target, str) and target.strip()
        ]
        expected_targets = {target.lower() for target in expected_targets_ordered}
        available_after = {str(col).strip().lower() for col in renamed_df.columns}
        missing_targets = sorted(expected_targets.difference(available_after))

        if missing_targets:
            raise ValueError(
                "CSV does not contain required mapped columns after rename. "
                f"missing={missing_targets}"
            )

        available_by_lower = {
            str(col).strip().lower(): col
            for col in renamed_df.columns
        }
        selected_columns: list[str] = []
        for target in expected_targets_ordered:
            actual_column = available_by_lower.get(target.lower())
            if actual_column is not None and actual_column not in selected_columns:
                selected_columns.append(actual_column)

        return renamed_df.loc[:, selected_columns].copy()

    def _validate_patterns(self, df: pd.DataFrame, config: dict[str, Any]) -> ValidationSummary:
        hints = config.get("content_hints")
        if not isinstance(hints, dict):
            return ValidationSummary(
                is_valid=True,
                score=1.0,
                threshold=self.validation_min_score,
                total_rules=0,
                failed_rules=0,
            )

        rules = hints.get("column_value_patterns")
        if not isinstance(rules, list) or not rules:
            return ValidationSummary(
                is_valid=True,
                score=1.0,
                threshold=self.validation_min_score,
                total_rules=0,
                failed_rules=0,
            )

        raw_sample_size = hints.get("sample_size", 200)
        try:
            sample_size = max(1, int(raw_sample_size))
        except (TypeError, ValueError):
            sample_size = 200

        normalized_to_actual = {str(column).strip().lower(): column for column in df.columns}

        weighted_total = 0.0
        weighted_match = 0.0
        failed_rules = 0
        evaluated_rules = 0

        for rule in rules:
            if not isinstance(rule, dict):
                continue

            raw_column = rule.get("column")
            raw_pattern = rule.get("pattern")
            if not isinstance(raw_column, str) or not raw_column.strip():
                continue
            if not isinstance(raw_pattern, str) or not raw_pattern.strip():
                continue

            try:
                weight = float(rule.get("weight", 1.0))
            except (TypeError, ValueError):
                weight = 1.0
            if weight <= 0:
                continue

            try:
                regex = re.compile(raw_pattern, re.IGNORECASE)
            except re.error as err:
                raise ValueError(
                    f"Invalid regex pattern in config for column {raw_column!r}: {err}"
                ) from err

            evaluated_rules += 1
            weighted_total += weight

            actual_column = normalized_to_actual.get(raw_column.strip().lower())
            if actual_column is None:
                failed_rules += 1
                continue

            values = df[actual_column].dropna().astype(str).head(sample_size)
            if values.empty:
                failed_rules += 1
                continue

            match_ratio = float(values.str.contains(regex, na=False).mean())
            weighted_match += match_ratio * weight

            if match_ratio < 0.7:
                failed_rules += 1

        if weighted_total <= 0:
            return ValidationSummary(
                is_valid=True,
                score=1.0,
                threshold=self.validation_min_score,
                total_rules=0,
                failed_rules=0,
            )

        score = weighted_match / weighted_total
        return ValidationSummary(
            is_valid=score >= self.validation_min_score,
            score=score,
            threshold=self.validation_min_score,
            total_rules=evaluated_rules,
            failed_rules=failed_rules,
        )

    def _split_by_date(self, df: pd.DataFrame, split_date_column: str) -> dict[date, pd.DataFrame]:
        normalized_to_actual = {str(column).strip().lower(): column for column in df.columns}
        actual_column = normalized_to_actual.get(split_date_column.strip().lower())
        if actual_column is None:
            raise ValueError(f"split_date_column not present in CSV: {split_date_column}")

        parsed = pd.to_datetime(df[actual_column], errors="coerce")
        valid_mask = parsed.notna()
        if not bool(valid_mask.any()):
            return {}

        with_dates = df.loc[valid_mask].copy()
        with_dates["_split_date"] = parsed.loc[valid_mask].dt.date

        frames: dict[date, pd.DataFrame] = {}
        for split_day, grouped in with_dates.groupby("_split_date", sort=True):
            output_df = grouped.drop(columns=["_split_date"]).reset_index(drop=True)
            # Normalize the split date column to a canonical upload format.
            output_df[actual_column] = pd.to_datetime(
                output_df[actual_column], errors="coerce"
            ).dt.strftime("%Y-%m-%d")
            frames[split_day] = output_df

        return frames


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


def default_naming_rules() -> dict[str, dict[str, Any]]:
    return {
        "contact": {
            "match_patterns": [r".*contact.*\\.csv$"],
            "output_stem": "contact_fact",
            "default_file_type": "fact",
        },
        "ev": {
            "match_patterns": [r".*ev.*\\.csv$", r".*electric.*\\.csv$"],
            "output_stem": "ev_fact",
            "default_file_type": "fact",
        },
        "print": {
            "match_patterns": [r".*print.*\\.csv$"],
            "output_stem": "print_fact",
            "default_file_type": "fact",
        },
    }
