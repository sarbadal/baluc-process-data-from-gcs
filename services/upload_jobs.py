from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
import tempfile
import threading
from typing import Any
from zoneinfo import ZoneInfo

from google.cloud import storage
from werkzeug.datastructures import FileStorage

from .upload_service import UploadRequest, UploadResult, UploadService


IST = ZoneInfo("Asia/Kolkata")


@dataclass(frozen=True)
class UploadCsvContentParams:
    storage_client: storage.Client
    target_bucket: str
    destination_path: str
    csv_content: str
    content_type: str = "text/csv"


@dataclass(frozen=True)
class ActiveResultUpdateParams:
    job_id: str
    filename: str
    category: str | None
    status_label: str
    message: str
    total_outputs: int = 0
    uploaded_count: int = 0
    uploaded_path: str | None = None


@dataclass(frozen=True)
class RunJobParams:
    job_id: str
    prepared_files: list[dict[str, str]]
    categories: list[str]
    file_types: list[str]
    upload_service: UploadService


def upload_csv_content(params: UploadCsvContentParams) -> str:
    """Upload CSV content and return the gs:// URI."""
    bucket = params.storage_client.bucket(params.target_bucket)
    blob = bucket.blob(params.destination_path)
    blob.upload_from_string(params.csv_content, content_type=params.content_type)
    return f"gs://{params.target_bucket}/{params.destination_path}"


class UploadJobCanceled(Exception):
    pass


class UploadJobManager:
    def __init__(self) -> None:
        self._jobs: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()

    def start_job(self, files: list[FileStorage], categories: list[str], file_types: list[str], upload_service: UploadService) -> str:
        job_id = datetime.now().strftime("%Y%m%d%H%M%S%f")
        prepared_files = self._materialize_uploads(files)

        with self._lock:
            self._jobs[job_id] = {
                "job_id": job_id,
                "status": "queued",
                "cancel_requested": False,
                "total_files": len(prepared_files),
                "processed_files": 0,
                "current_file": None,
                "current_step": "Waiting to start",
                "active_results": {},
                "results": [],
                "started_at": self._now_ist_iso(),
                "finished_at": None,
            }

        worker = threading.Thread(
            target=self._run_job,
            args=(
                RunJobParams(
                    job_id=job_id,
                    prepared_files=prepared_files,
                    categories=categories,
                    file_types=file_types,
                    upload_service=upload_service,
                ),
            ),
            daemon=True,
        )
        worker.start()
        return job_id

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return None
            return {
                "job_id": job["job_id"],
                "status": job["status"],
                "cancel_requested": job["cancel_requested"],
                "total_files": job["total_files"],
                "processed_files": job["processed_files"],
                "current_file": job["current_file"],
                "current_step": job["current_step"],
                "active_results": list(job["active_results"].values()),
                "results": list(job["results"]),
                "started_at": job["started_at"],
                "finished_at": job["finished_at"],
            }

    def cancel_job(self, job_id: str) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return False
            if job["status"] in {"completed", "failed", "canceled"}:
                return True
            job["cancel_requested"] = True
            job["status"] = "canceling"
            job["current_step"] = "Cancel requested. Stopping upload..."
            return True

    def _materialize_uploads(self, files: list[FileStorage]) -> list[dict[str, str]]:
        prepared: list[dict[str, str]] = []
        for file in files:
            filename = (file.filename or "").strip()
            if not filename:
                continue
            suffix = Path(filename).suffix or ".csv"
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                file.stream.seek(0)
                tmp.write(file.stream.read())
                temp_path = tmp.name

            prepared.append(
                {
                    "temp_path": temp_path,
                    "filename": filename,
                    "content_type": file.content_type or "text/csv",
                }
            )
        return prepared

    def _run_job(self, params: RunJobParams) -> None:
        self._set_job_state(params.job_id, status="running", current_step="Processing files")

        try:
            for idx, item in enumerate(params.prepared_files):
                self._raise_if_cancel_requested(params.job_id)
                chosen_category = params.categories[idx] if idx < len(params.categories) else ""
                normalized_category = chosen_category.strip() or None
                chosen_file_type = params.file_types[idx] if idx < len(params.file_types) else "fact"
                self._set_job_state(
                    params.job_id,
                    current_file=item["filename"],
                    current_step=f"Reading {item['filename']}",
                )

                with open(item["temp_path"], "rb") as stream:
                    file = FileStorage(
                        stream=stream,
                        filename=item["filename"],
                        content_type=item["content_type"],
                    )
                    result = params.upload_service.handle_upload(
                        UploadRequest(
                            file=file,
                            chosen_category=normalized_category,
                            file_type=chosen_file_type,
                            progress_callback=lambda event: self._on_progress(params.job_id, event),
                        )
                    )

                self._raise_if_cancel_requested(params.job_id)

                self._append_result(params.job_id, result)
                self._set_job_state(
                    params.job_id,
                    processed_files=idx + 1,
                    current_step="File completed",
                )

            self._set_job_state(
                params.job_id,
                status="completed",
                current_step="Completed",
                finished_at=self._now_ist_iso(),
            )
        except UploadJobCanceled:
            self._set_job_state(
                params.job_id,
                status="canceled",
                current_step="Canceled by user.",
                finished_at=self._now_ist_iso(),
            )
        except Exception as exc:
            self._set_job_state(
                params.job_id,
                status="failed",
                current_step=f"Failed: {exc}",
                finished_at=self._now_ist_iso(),
            )
        finally:
            for item in params.prepared_files:
                try:
                    Path(item["temp_path"]).unlink(missing_ok=True)
                except OSError:
                    pass

    def _append_result(self, job_id: str, result: UploadResult) -> None:
        with self._lock:
            if job_id not in self._jobs:
                return
            key = self._active_key(result.original_filename, result.category)
            self._jobs[job_id]["active_results"].pop(key, None)
            self._jobs[job_id]["results"].append(asdict(result))

    def _on_progress(self, job_id: str, event: dict[str, Any]) -> None:
        step = event.get("event", "processing")
        destination_path = event.get("destination_path")
        filename = str(event.get("filename", "")).strip()
        category = str(event.get("category", "")).strip() or None

        if step == "frames_ready":
            message = (
                f"Preparing {event.get('total_outputs', 0)} output file(s) for "
                f"{event.get('filename', '')}"
            )
            self._upsert_active_result(ActiveResultUpdateParams(
                job_id=job_id,
                filename=filename,
                category=category,
                status_label="In Progress",
                message=message,
                total_outputs=int(event.get("total_outputs", 0) or 0),
            ))
        elif step == "uploading_output":
            message = (
                f"Uploading {event.get('output_index', 0)}/{event.get('total_outputs', 0)}"
            )
            if destination_path:
                message = f"{message}: {destination_path}"
            self._upsert_active_result(ActiveResultUpdateParams(
                job_id=job_id,
                filename=filename,
                category=category,
                status_label="In Progress",
                message=message,
                total_outputs=int(event.get("total_outputs", 0) or 0),
                uploaded_count=max(int(event.get("output_index", 0) or 0) - 1, 0),
            ))
        elif step == "uploaded_output":
            message = (
                f"Uploaded {event.get('output_index', 0)}/{event.get('total_outputs', 0)}"
            )
            if destination_path:
                message = f"{message}: {destination_path}"
            self._upsert_active_result(ActiveResultUpdateParams(
                job_id=job_id,
                filename=filename,
                category=category,
                status_label="In Progress",
                message=message,
                total_outputs=int(event.get("total_outputs", 0) or 0),
                uploaded_count=int(event.get("output_index", 0) or 0),
                uploaded_path=destination_path,
            ))
        else:
            message = step

        self._set_job_state(job_id, current_step=message)
        self._raise_if_cancel_requested(job_id)

    def _set_job_state(self, job_id: str, **updates: Any) -> None:
        with self._lock:
            if job_id not in self._jobs:
                return
            self._jobs[job_id].update(updates)

    def _raise_if_cancel_requested(self, job_id: str) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return
            if job.get("cancel_requested"):
                raise UploadJobCanceled()

    @staticmethod
    def _active_key(filename: str, category: str | None) -> str:
        return f"{filename}::{category or ''}"

    @staticmethod
    def _now_ist_iso() -> str:
        return datetime.now(IST).isoformat(timespec="seconds")

    def _upsert_active_result(self, params: ActiveResultUpdateParams) -> None:
        if not params.filename:
            return

        key = self._active_key(params.filename, params.category)
        with self._lock:
            if params.job_id not in self._jobs:
                return

            active_results = self._jobs[params.job_id]["active_results"]
            current = active_results.get(
                key,
                {
                    "success": False,
                    "requires_category": False,
                    "message": params.message,
                    "original_filename": params.filename,
                    "category": params.category,
                    "destination_path": None,
                    "storage_uri": None,
                    "uploaded_paths": [],
                    "uploaded_uris": [],
                    "status_label": params.status_label,
                    "progress": "0/0",
                    "is_active": True,
                },
            )

            if params.uploaded_path:
                existing_paths = list(current.get("uploaded_paths") or [])
                if params.uploaded_path not in existing_paths:
                    existing_paths.append(params.uploaded_path)
                current["uploaded_paths"] = existing_paths

            current["message"] = params.message
            current["status_label"] = params.status_label
            if params.total_outputs > 0:
                current["progress"] = f"{params.uploaded_count}/{params.total_outputs}"

            active_results[key] = current
