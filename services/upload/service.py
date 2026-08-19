from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Optional

from werkzeug.datastructures import FileStorage


@dataclass
class UploadRequest:
    file: FileStorage
    chosen_category: Optional[str] = None
    file_type: str = "fact"
    progress_callback: Optional[Callable[[dict], None]] = None


@dataclass
class UploadResult:
    success: bool
    requires_category: bool
    message: str
    original_filename: str
    category: Optional[str] = None
    destination_path: Optional[str] = None
    storage_uri: Optional[str] = None
    uploaded_paths: list[str] = field(default_factory=list)
    uploaded_uris: list[str] = field(default_factory=list)


class UploadService:
    """Base upload service for compatibility with UploadJobManager workflows."""

    def handle_upload(self, request: UploadRequest) -> UploadResult:
        raise NotImplementedError("UploadService.handle_upload must be implemented by consumers.")
