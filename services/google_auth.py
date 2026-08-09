from __future__ import annotations

from dataclasses import dataclass
import logging
from pathlib import Path
from typing import Optional

from google.auth.credentials import Credentials
from google.cloud import storage
from google.oauth2 import service_account


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class GoogleAuthContext:
    project_id: str
    credentials: Optional[Credentials]
    auth_method: str
    key_path: str


def _resolve_key_path(configured_key_path: str, base_dir: str | Path) -> Path:
    candidate = Path(configured_key_path).expanduser()
    if candidate.is_absolute():
        return candidate
    return Path(base_dir).joinpath(candidate)


def build_google_auth_context(project_id: str, configured_key_path: str, base_dir: str | Path) -> GoogleAuthContext:
    if not project_id.strip():
        raise RuntimeError("GOOGLE_CLOUD_PROJECT_ID must be configured.")

    resolved_key_path = _resolve_key_path(configured_key_path, base_dir)

    if resolved_key_path.is_file():
        credentials = service_account.Credentials.from_service_account_file(
            str(resolved_key_path)
        )
        LOGGER.info(
            "Using service-account credentials from configured key file. project_id=%s key_path=%s",
            project_id,
            resolved_key_path,
        )
        return GoogleAuthContext(
            project_id=project_id,
            credentials=credentials,
            auth_method="service_account_key_file",
            key_path=str(resolved_key_path),
        )

    LOGGER.info(
        "Configured key file not found. Falling back to Google Application Default Credentials. "
        "project_id=%s key_path=%s",
        project_id,
        resolved_key_path,
    )
    return GoogleAuthContext(
        project_id=project_id,
        credentials=None,
        auth_method="application_default_credentials",
        key_path=str(resolved_key_path),
    )


def create_storage_client(auth_context: GoogleAuthContext) -> storage.Client:
    return storage.Client(
        project=auth_context.project_id,
        credentials=auth_context.credentials,
    )
