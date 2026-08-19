from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
import re
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class ValidationSummary:
    is_valid: bool
    score: float
    threshold: float
    total_rules: int
    failed_rules: int


@dataclass(frozen=True)
class ValidationTotals:
    weighted_total: float
    weighted_match: float
    evaluated_rules: int
    failed_rules: int


@dataclass
class DataFramePreparationService:
    validation_min_score: float
    _validation_df: pd.DataFrame | None = field(init=False, default=None, repr=False)
    _validation_sample_size: int = field(init=False, default=200, repr=False)
    _validation_normalized_to_actual: dict[str, str] = field(init=False, default_factory=dict, repr=False)

    def rename_columns(self, input_df: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
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

        return renamed_df

    def select_mapped_columns(self, df: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
        field_mapping = config.get("field_mapping")
        if not isinstance(field_mapping, dict) or not field_mapping:
            raise ValueError("Invalid or empty field_mapping in category config")

        expected_targets_ordered = [
            str(target).strip()
            for target in field_mapping.keys()
            if isinstance(target, str) and target.strip()
        ]

        available_by_lower = {
            str(col).strip().lower(): col
            for col in df.columns
        }
        selected_columns: list[str] = []
        for target in expected_targets_ordered:
            actual_column = available_by_lower.get(target.lower())
            if actual_column is not None and actual_column not in selected_columns:
                selected_columns.append(actual_column)

        return df.loc[:, selected_columns].copy()

    def validate_patterns(self, df: pd.DataFrame, config: dict[str, Any]) -> ValidationSummary:
        hints = config.get("content_hints")
        rules = self._extract_pattern_rules(hints)
        if not rules:
            return self._empty_validation_summary()

        self._validation_df = df
        self._validation_sample_size = self._resolve_sample_size(hints)
        self._validation_normalized_to_actual = {
            str(column).strip().lower(): column
            for column in df.columns
        }

        w_total = 0.0
        w_match = 0.0
        failed_rules = 0
        eval_rules = 0

        for rule in rules:
            metrics = self._evaluate_pattern_rule(rule=rule)
            if metrics is None:
                continue

            eval_rules += 1
            w_total += metrics["weight"]
            w_match += metrics["weighted_match"]
            failed_rules += metrics["failed"]

        totals = ValidationTotals(
            weighted_total=w_total,
            weighted_match=w_match,
            evaluated_rules=eval_rules,
            failed_rules=failed_rules,
        )
        return self._build_validation_summary(totals)

    def _extract_pattern_rules(self, hints: Any) -> list[dict[str, Any]]:
        if not isinstance(hints, dict):
            return []

        rules = hints.get("column_value_patterns")
        if rules is None:
            rules = hints.get("column_value_pattern")
        if not isinstance(rules, list) or not rules:
            return []
        return rules

    def _resolve_sample_size(self, hints: Any) -> int:
        raw_sample_size = 200
        if isinstance(hints, dict):
            raw_sample_size = hints.get("sample_size", 200)

        try:
            return max(1, int(raw_sample_size))
        except (TypeError, ValueError):
            return 200

    def _evaluate_pattern_rule(self, rule: dict[str, Any]) -> dict[str, float | int] | None:
        if self._validation_df is None:
            raise ValueError("Validation dataframe is not initialized")

        if not isinstance(rule, dict):
            return None

        raw_column = rule.get("column")
        raw_pattern = rule.get("pattern")
        if not isinstance(raw_column, str) or not raw_column.strip():
            return None
        if not isinstance(raw_pattern, str) or not raw_pattern.strip():
            return None

        try:
            weight = float(rule.get("weight", 1.0))
        except (TypeError, ValueError):
            weight = 1.0
        if weight <= 0:
            return None

        try:
            regex = re.compile(raw_pattern, re.IGNORECASE)
        except re.error as err:
            raise ValueError(
                f"Invalid regex pattern in config for column {raw_column!r}: {err}"
            ) from err

        actual_column = self._validation_normalized_to_actual.get(raw_column.strip().lower())
        if actual_column is None:
            return {"weight": weight, "weighted_match": 0.0, "failed": 1}

        values = self._validation_df[actual_column].dropna().astype(str).head(self._validation_sample_size)
        if values.empty:
            return {"weight": weight, "weighted_match": 0.0, "failed": 1}

        match_ratio = float(values.str.contains(regex, na=False).mean())
        failed = 1 if match_ratio < 0.7 else 0
        return {
            "weight": weight,
            "weighted_match": match_ratio * weight,
            "failed": failed,
        }

    def _build_validation_summary(self, totals: ValidationTotals) -> ValidationSummary:
        if totals.weighted_total <= 0:
            return self._empty_validation_summary()

        score = totals.weighted_match / totals.weighted_total
        return ValidationSummary(
            is_valid=score >= self.validation_min_score,
            score=score,
            threshold=self.validation_min_score,
            total_rules=totals.evaluated_rules,
            failed_rules=totals.failed_rules,
        )

    def _empty_validation_summary(self) -> ValidationSummary:
        return ValidationSummary(
            is_valid=True,
            score=1.0,
            threshold=self.validation_min_score,
            total_rules=0,
            failed_rules=0,
        )

    def split_by_date(self, df: pd.DataFrame, split_date_column: str) -> dict[date, pd.DataFrame]:
        actual_column = self._resolve_split_column(df, split_date_column)
        parsed_dates = self._parse_split_dates(df, actual_column)
        valid_mask = parsed_dates.notna()
        if not bool(valid_mask.any()):
            return {}

        with_dates = self._build_split_source_frame(df, parsed_dates, valid_mask)

        frames: dict[date, pd.DataFrame] = {}
        for split_day, grouped in with_dates.groupby("_split_date", sort=True):
            frames[split_day] = self._build_split_output_frame(grouped, actual_column)

        return frames

    def _resolve_split_column(self, df: pd.DataFrame, split_date_column: str) -> str:
        normalized_to_actual = {str(column).strip().lower(): column for column in df.columns}
        actual_column = normalized_to_actual.get(split_date_column.strip().lower())
        if actual_column is None:
            raise ValueError(f"split_date_column not present in CSV: {split_date_column}")
        return actual_column

    def _parse_split_dates(self, df: pd.DataFrame, actual_column: str) -> pd.Series:
        return pd.to_datetime(df[actual_column], errors="coerce")

    def _build_split_source_frame(self, df: pd.DataFrame, parsed_dates: pd.Series, valid_mask: pd.Series) -> pd.DataFrame:
        with_dates = df.loc[valid_mask].copy()
        with_dates["_split_date"] = parsed_dates.loc[valid_mask].dt.date
        return with_dates

    def _build_split_output_frame(self, grouped: pd.DataFrame, actual_column: str) -> pd.DataFrame:
        output_df = grouped.drop(columns=["_split_date"]).reset_index(drop=True)
        # Normalize the split date column to a canonical upload format.
        output_df[actual_column] = pd.to_datetime(
            output_df[actual_column], errors="coerce"
        ).dt.strftime("%Y-%m-%d")
        return output_df

    @staticmethod
    def parse_gcs_path(gcs_path: str) -> tuple[str, str]:
        if not gcs_path.startswith("gs://"):
            raise ValueError(f"Invalid gcs_path (expected gs://...): {gcs_path}")

        bucket_and_path = gcs_path[5:]
        bucket, _, object_path = bucket_and_path.partition("/")
        bucket = bucket.strip()
        object_path = object_path.strip("/ ")
        if not bucket or not object_path:
            raise ValueError(f"Invalid gcs_path (missing bucket or object path): {gcs_path}")
        return bucket, object_path
