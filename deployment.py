from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
from pathlib import Path


REQUIRED_ENV_KEYS = (
    "GOOGLE_CLOUD_PROJECT_ID",
    "SOURCE_BUCKET",
    "TARGET_BUCKET",
)


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


def build_gcloud_command(args: argparse.Namespace, env_values: dict[str, str]) -> list[str]:
    set_env_vars = {
        "GOOGLE_CLOUD_PROJECT_ID": env_values["GOOGLE_CLOUD_PROJECT_ID"],
        "SOURCE_BUCKET": env_values["SOURCE_BUCKET"],
        "TARGET_BUCKET": env_values["TARGET_BUCKET"],
        "VALIDATION_MIN_SCORE": env_values.get("VALIDATION_MIN_SCORE", "0.7"),
    }

    google_auth_key_path = env_values.get("GOOGLE_AUTH_KEY_PATH", "").strip()
    if google_auth_key_path:
        set_env_vars["GOOGLE_AUTH_KEY_PATH"] = google_auth_key_path

    env_vars_flag = ",".join(f"{key}={value}" for key, value in set_env_vars.items())

    return [
        "gcloud",
        "functions",
        "deploy",
        args.function_name,
        "--gen2",
        f"--runtime={args.runtime}",
        f"--region={args.region}",
        f"--source={args.source}",
        f"--entry-point={args.entry_point}",
        f"--trigger-bucket={env_values['SOURCE_BUCKET']}",
        f"--project={env_values['GOOGLE_CLOUD_PROJECT_ID']}",
        f"--set-env-vars={env_vars_flag}",
    ]


def run_deploy(command: list[str], dry_run: bool) -> int:
    print("Deployment command:")
    print(shlex.join(command))

    if dry_run:
        print("Dry run enabled. Command not executed.")
        return 0

    result = subprocess.run(command, check=False)
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
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    env_file = Path(args.env_file).expanduser().resolve()

    try:
        env_values = parse_env_file(env_file)
        validate_required_env(env_values)
        command = build_gcloud_command(args, env_values)
        return run_deploy(command, args.dry_run)
    except Exception as exc:
        print(f"Deployment failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

# python deployment.py --env-file deploy.env
