from __future__ import annotations

import logging
from pathlib import Path

from services.processing.bootstrap import get_processor, get_source_bucket


logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent


def process_gcs_file(event: dict, _context: object) -> None:
    """Cloud Function entry point for GCS object finalize events."""

    source_bucket = get_source_bucket()

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

    processor = get_processor(PROJECT_ROOT)
    outputs = processor.process_uploaded_object(source_bucket=event_bucket, object_name=object_name)

    LOGGER.info(
        "Generated %d split file(s) for gs://%s/%s",
        len(outputs),
        event_bucket,
        object_name,
    )
