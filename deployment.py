from __future__ import annotations

import argparse
from dataclasses import dataclass
import shlex
import subprocess
import sys
from pathlib import Path


REQUIRED_ENV_KEYS = (
    "GOOGLE_CLOUD_PROJECT_ID",
    "SOURCE_BUCKET",
    "TARGET_BUCKET",
)


@dataclass(frozen=True)
class DeployContext:
    project_id: str
    source_bucket: str
    target_bucket: str
    validation_min_score: str
    google_auth_key_path: str
    function_name: str
    entry_point: str
    region: str
    trigger_location_override: str
    runtime: str
    source_dir: str
    dry_run: bool
    auto_grant_pubsub_publisher: bool


def parse_env_file(env_file: Path) -> dict[str, str]:
    if not env_file.is_file():
        raise FileNotFoundError(f"Env file not found: {env_file}")

    values: dict[str, str] = {}
    for line_number, raw_line in enumerate(env_file.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        if "=" not in line:
            raise ValueError(
                f"Invalid env entry at {env_file}:{line_number}. Expected KEY=VALUE format."
            )

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()

        if not key:
            raise ValueError(f"Invalid env key at {env_file}:{line_number}")

        # Strip optional wrapping quotes in values.
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]

        values[key] = value

    return values


def validate_required_env(values: dict[str, str]) -> None:
    missing = [key for key in REQUIRED_ENV_KEYS if not values.get(key, "").strip()]
    if missing:
        raise ValueError(
            "Missing required deployment variables in env file: " + ", ".join(missing)
        )


def build_deploy_context(args: argparse.Namespace, env_values: dict[str, str]) -> DeployContext:
    return DeployContext(
        project_id=env_values["GOOGLE_CLOUD_PROJECT_ID"].strip(),
        source_bucket=env_values["SOURCE_BUCKET"].strip(),
        target_bucket=env_values["TARGET_BUCKET"].strip(),
        validation_min_score=env_values.get("VALIDATION_MIN_SCORE", "0.7").strip() or "0.7",
        google_auth_key_path=env_values.get("GOOGLE_AUTH_KEY_PATH", "").strip(),
        function_name=str(args.function_name).strip(),
        entry_point=str(args.entry_point).strip(),
        region=str(args.region).strip(),
        trigger_location_override=str(args.trigger_location or "").strip().lower(),
        runtime=str(args.runtime).strip(),
        source_dir=str(args.source).strip(),
        dry_run=bool(args.dry_run),
        auto_grant_pubsub_publisher=bool(args.auto_grant_pubsub_publisher),
    )


def detect_bucket_location(project_id: str, source_bucket: str) -> str | None:
    command = [
        "gcloud",
        "storage",
        "buckets",
        "describe",
        f"gs://{source_bucket}",
        f"--project={project_id}",
        "--format=value(location)",
    ]

    result = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return None

    location = result.stdout.strip().lower()
    return location or None


def detect_project_number(project_id: str) -> str | None:
    command = [
        "gcloud",
        "projects",
        "describe",
        project_id,
        "--format=value(projectNumber)",
    ]
    result = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return None

    project_number = result.stdout.strip()
    return project_number or None


def gcs_service_account_member(project_number: str) -> str:
    email = f"service-{project_number}@gs-project-accounts.iam.gserviceaccount.com"
    return f"serviceAccount:{email}"


def print_pubsub_publisher_remediation(project_id: str) -> None:
    project_number = detect_project_number(project_id)
    if not project_number:
        print(
            "Could not auto-detect project number for remediation. "
            "Run: gcloud projects describe <PROJECT_ID> --format='value(projectNumber)'"
        )
        return

    member = gcs_service_account_member(project_number)
    print("\nRemediation for Eventarc trigger permissions:")
    print(
        "gcloud projects add-iam-policy-binding "
        f"{project_id} --member='{member}' --role='roles/pubsub.publisher'"
    )


def maybe_auto_grant_pubsub_publisher(
    context: DeployContext,
) -> None:
    if not context.auto_grant_pubsub_publisher:
        return

    project_number = detect_project_number(context.project_id)
    if not project_number:
        raise RuntimeError(
            "Failed to auto-grant IAM role because project number could not be determined."
        )

    member = gcs_service_account_member(project_number)
    command = [
        "gcloud",
        "projects",
        "add-iam-policy-binding",
        context.project_id,
        f"--member={member}",
        "--role=roles/pubsub.publisher",
        "--quiet",
    ]

    print("Ensuring Pub/Sub Publisher role for GCS service account:")
    print(shlex.join(command))

    if context.dry_run:
        print("Dry run enabled. IAM binding command not executed.")
        return

    result = subprocess.run(command, check=False)
    if result.returncode != 0:
        raise RuntimeError(
            "Failed to grant roles/pubsub.publisher to GCS service account. "
            "Run the printed command manually with sufficient IAM permissions."
        )


def resolve_trigger_location(context: DeployContext, env_values: dict[str, str]) -> str:
    env_trigger_location = env_values.get("TRIGGER_LOCATION", "").strip().lower()
    if env_trigger_location:
        return env_trigger_location

    if context.trigger_location_override:
        return context.trigger_location_override

    detected = detect_bucket_location(
        project_id=context.project_id,
        source_bucket=context.source_bucket,
    )
    if detected:
        print(
            "Auto-detected TRIGGER_LOCATION from source bucket "
            f"{context.source_bucket}: {detected}"
        )
        return detected

    raise ValueError(
        "Unable to determine TRIGGER_LOCATION automatically. "
        "Set TRIGGER_LOCATION in env file or pass --trigger-location."
    )


def build_gcloud_command(context: DeployContext, env_values: dict[str, str]) -> list[str]:
    trigger_location = resolve_trigger_location(context, env_values)

    set_env_vars = {
        "GOOGLE_CLOUD_PROJECT_ID": context.project_id,
        "SOURCE_BUCKET": context.source_bucket,
        "TARGET_BUCKET": context.target_bucket,
        "VALIDATION_MIN_SCORE": context.validation_min_score,
    }

    if context.google_auth_key_path:
        set_env_vars["GOOGLE_AUTH_KEY_PATH"] = context.google_auth_key_path

    env_vars_flag = ",".join(f"{key}={value}" for key, value in set_env_vars.items())

    return [
        "gcloud",
        "functions",
        "deploy",
        context.function_name,
        "--gen2",
        f"--runtime={context.runtime}",
        f"--region={context.region}",
        f"--source={context.source_dir}",
        f"--entry-point={context.entry_point}",
        f"--trigger-bucket={context.source_bucket}",
        f"--trigger-location={trigger_location}",
        f"--project={context.project_id}",
        f"--set-env-vars={env_vars_flag}",
    ]


def run_deploy(command: list[str], context: DeployContext) -> int:
    print("Deployment command:")
    print(shlex.join(command))

    if context.dry_run:
        print("Dry run enabled. Command not executed.")
        return 0

    result = subprocess.run(command, check=False, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)

    if result.returncode != 0:
        error_text = f"{result.stdout}\n{result.stderr}".lower()
        if "roles/pubsub.publisher" in error_text or (
            "cloud storage service account" in error_text and "publish" in error_text
        ):
            print_pubsub_publisher_remediation(context.project_id)

    return result.returncode


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Deploy the Cloud Function using values from an env file."
    )
    parser.add_argument(
        "--env-file",
        required=True,
        help="Path to env file, for example deploy.env",
    )
    parser.add_argument(
        "--function-name",
        default="process-gcs-file",
        help="Cloud Function name to deploy.",
    )
    parser.add_argument(
        "--entry-point",
        default="process_gcs_file",
        help="Python entry point function name.",
    )
    parser.add_argument(
        "--region",
        default="us-central1",
        help="GCP region for deployment.",
    )
    parser.add_argument(
        "--trigger-location",
        default="",
        help=(
            "Eventarc trigger location. Must match SOURCE_BUCKET location "
            "(for example: us, us-central1, asia-south1)."
        ),
    )
    parser.add_argument(
        "--runtime",
        default="python312",
        help="Cloud Functions runtime.",
    )
    parser.add_argument(
        "--source",
        default=".",
        help="Function source directory.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the gcloud command without executing deployment.",
    )
    parser.add_argument(
        "--auto-grant-pubsub-publisher",
        action="store_true",
        help=(
            "Grant roles/pubsub.publisher to the GCS service account before deploy. "
            "Requires IAM permission to modify project policy."
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    env_file = Path(args.env_file).expanduser().resolve()

    try:
        env_values = parse_env_file(env_file)
        validate_required_env(env_values)
        context = build_deploy_context(args, env_values)
        maybe_auto_grant_pubsub_publisher(context)
        command = build_gcloud_command(context, env_values)
        return run_deploy(command, context)
    except Exception as exc:
        print(f"Deployment failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

# 
