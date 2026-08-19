from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Dict, Iterable, Optional


@dataclass(frozen=True)
class FileTypeRule:
    category: str
    match_patterns: tuple[str, ...]
    output_stem: str
    default_file_type: str


class FilenameConventionService:
    def __init__(self, raw_rules: Dict[str, dict]) -> None:
        self._rules: Dict[str, FileTypeRule] = {}
        self._compiled_patterns: Dict[str, tuple[re.Pattern[str], ...]] = {}

        for category, definition in raw_rules.items():
            default_file_type = str(definition.get("default_file_type", "fact")).strip().lower()
            if default_file_type not in {"fact", "dim"}:
                default_file_type = "fact"

            rule = FileTypeRule(
                category=category,
                match_patterns=tuple(definition.get("match_patterns", [])),
                output_stem=definition["output_stem"],
                default_file_type=default_file_type,
            )
            self._rules[category] = rule
            self._compiled_patterns[category] = tuple(
                re.compile(pattern, re.IGNORECASE) for pattern in rule.match_patterns
            )

    def categories(self) -> Iterable[str]:
        return self._rules.keys()

    def has_category(self, category: str) -> bool:
        return category in self._rules

    def detect_category(self, filename: str) -> Optional[str]:
        for category, patterns in self._compiled_patterns.items():
            if any(pattern.match(filename) for pattern in patterns):
                return category
        return None

    def build_filename(self, category: str, when: date) -> str:
        if not self.has_category(category):
            raise ValueError(f"Unknown upload category: {category}")

        stem = self._rules[category].output_stem
        return f"{stem}_{when.strftime('%Y%m%d')}.csv"

    def default_file_type_for_category(self, category: str) -> str:
        rule = self._rules.get(category)
        if rule is None:
            return "fact"
        return rule.default_file_type

    def category_default_file_types(self) -> dict[str, str]:
        return {
            category: rule.default_file_type
            for category, rule in self._rules.items()
        }
