from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Optional

import pandas as pd

from .naming import FilenameConventionService


@dataclass(frozen=True)
class CategoryMatch:
    category: str
    source: str
    confidence: float


@dataclass
class CategoryDetectionService:
    naming_service: FilenameConventionService
    processing_category_configs: dict[str, dict]
    min_confidence: float = 0.45
    tie_break_delta: float = 0.05

    def detect_category(self, filename: str, df: pd.DataFrame | None = None) -> Optional[CategoryMatch]:
        filename_category = self.naming_service.detect_category(filename)
        if filename_category:
            return CategoryMatch(category=filename_category, source="filename", confidence=1.0)

        if df is None:
            return None

        return self._detect_from_dataframe(df)

    def category_match_score(self, category: str, df: pd.DataFrame) -> float | None:
        config = self.processing_category_configs.get(category)
        if not isinstance(config, dict):
            return None

        normalized_columns = {str(column).strip().lower() for column in df.columns}
        return self._score_category(df, normalized_columns, config)

    def category_matches(self, category: str, df: pd.DataFrame) -> tuple[bool, float | None]:
        score = self.category_match_score(category, df)
        if score is None:
            return False, None
        return score >= self.min_confidence, score

    def _detect_from_dataframe(self, df: pd.DataFrame) -> Optional[CategoryMatch]:
        normalized_columns = {str(column).strip().lower() for column in df.columns}
        ranked: list[tuple[float, str]] = []

        for category, config in self.processing_category_configs.items():
            score = self._score_category(df, normalized_columns, config)
            ranked.append((score, category))

        ranked.sort(reverse=True)
        if not ranked:
            return None

        top_score, top_category = ranked[0]
        if top_score < self.min_confidence:
            return None

        if len(ranked) > 1 and (top_score - ranked[1][0]) <= self.tie_break_delta:
            return None

        return CategoryMatch(category=top_category, source="csv_fields", confidence=top_score)

    def _score_category(self, df: pd.DataFrame, normalized_columns: set[str], config: dict) -> float:
        field_mapping = config.get("field_mapping")
        if not isinstance(field_mapping, dict) or not field_mapping:
            return 0.0

        source_columns = {
            str(source).strip().lower()
            for source in field_mapping.values()
            if isinstance(source, str) and source.strip()
        }
        target_columns = {
            str(target).strip().lower()
            for target in field_mapping.keys()
            if isinstance(target, str) and target.strip()
        }

        source_hits = len(source_columns.intersection(normalized_columns)) if source_columns else 0
        target_hits = len(target_columns.intersection(normalized_columns)) if target_columns else 0

        # Some incoming files may already use normalized field names, while others use raw source headers.
        best_hit_count = max(source_hits, target_hits)
        denominator = max(len(source_columns), len(target_columns), 1)
        header_score = best_hit_count / denominator

        # Optional content hints can push category confidence using sampled row values.
        content_score = self._score_content_hints(df, config)
        score = header_score
        if content_score is not None:
            score = (header_score * 0.75) + (content_score * 0.25)

        split_date_column = str(config.get("split_date_column", "")).strip().lower()
        if split_date_column and split_date_column in normalized_columns:
            score += 0.08

        return min(score, 1.0)

    def _score_content_hints(self, df: pd.DataFrame, config: dict) -> float | None:
        hints = config.get("content_hints")
        if not isinstance(hints, dict):
            return None

        pattern_rules = hints.get("column_value_patterns")
        if pattern_rules is None:
            pattern_rules = hints.get("column_value_pattern")
        if not isinstance(pattern_rules, list) or not pattern_rules:
            return None

        raw_sample_size = hints.get("sample_size", 200)
        try:
            sample_size = max(1, int(raw_sample_size))
        except (TypeError, ValueError):
            sample_size = 200

        normalized_to_actual = {
            str(column).strip().lower(): column
            for column in df.columns
        }

        weighted_total = 0.0
        weighted_match = 0.0

        for rule in pattern_rules:
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

            weighted_total += weight
            match_ratio = 0.0

            actual_column = normalized_to_actual.get(raw_column.strip().lower())
            if actual_column is not None:
                series = df[actual_column].dropna().astype(str).head(sample_size)
                if not series.empty:
                    try:
                        regex = re.compile(raw_pattern, re.IGNORECASE)
                    except re.error:
                        regex = None
                    if regex is not None:
                        match_ratio = float(series.str.contains(regex, na=False).mean())

            weighted_match += match_ratio * weight

        if weighted_total <= 0:
            return None

        return weighted_match / weighted_total
