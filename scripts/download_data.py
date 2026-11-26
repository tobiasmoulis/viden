#!/usr/bin/env python3
"""Download and verify the CorpTech Enterprise Lakehouse landing dataset.

Primary mode reads a SHA-256 manifest (default: `MANIFEST.sha256`) stored under
`R2_PREFIX` in Cloudflare R2, downloads every artifact listed in the manifest,
verifies SHA-256 checksums before extraction, unpacks archives into
`./data/landing`, and validates that all 10 required dataset folders exist.

Also supports direct HTTP archive downloads (`--url` / `DATASET_DOWNLOAD_URL`)
and local verification (`--verify-only`).
"""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import shutil
import sys
import tarfile
import tempfile
import urllib.request
import zipfile
from pathlib import Path
from typing import Any

REQUIRED_DATASETS: tuple[str, ...] = (
    "clients",
    "projects",
    "survey_questions",
    "employees",
    "timesheets",
    "fx_rates",
    "invoices",
    "expenses",
    "crm_events",
    "employee_surveys",
)

_MANIFEST_LINE_RE = re.compile(r"^([a-fA-F0-9]{64})\s+\*?(.+)$")
_ARCHIVE_SUFFIXES = (".zip", ".tar.gz", ".tgz")


def load_dotenv_if_present(env_file: Path) -> None:
    """Populate `os.environ` from a `.env` file without overriding existing vars."""
    if not env_file.is_file():
        return

    for raw_line in env_file.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        os.environ.setdefault(key, value)


def parse_sha256_manifest(content: str) -> list[tuple[str, str]]:
    """Parse `sha256sum`-formatted manifest text into `(hex_hash, relative_path)` pairs."""
    entries: list[tuple[str, str]] = []
    for line_no, raw_line in enumerate(content.splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = _MANIFEST_LINE_RE.match(line)
        if not match:
            raise ValueError(f"Invalid manifest entry at line {line_no}: {raw_line!r}")
        expected_hash = match.group(1).lower()
        rel_path = match.group(2).strip().replace("\\", "/")
        while rel_path.startswith("./"):
            rel_path = rel_path[2:]
        if not rel_path or rel_path.startswith("/") or ".." in Path(rel_path).parts:
            raise ValueError(f"Unsafe manifest path at line {line_no}: {rel_path!r}")
        entries.append((expected_hash, rel_path))

    if not entries:
        raise ValueError("SHA-256 manifest is empty; expected at least one entry.")
    return entries


def compute_sha256(file_path: Path) -> str:
    """Compute lowercase hexadecimal SHA-256 digest for `file_path`."""
    digest = hashlib.sha256()
    with file_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest().lower()


def create_r2_client(account_id: str, access_key: str, secret_key: str) -> Any:
    """Create a boto3 S3 client configured for Cloudflare R2."""
    import boto3
    from botocore.config import Config

    endpoint_url = f"https://{account_id}.r2.cloudflarestorage.com"
    try:
        config = Config(
            region_name="auto",
            signature_version="s3v4",
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        )
    except TypeError:
        config = Config(region_name="auto", signature_version="s3v4")

    return boto3.client(
        service_name="s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=config,
    )


def _discover_manifest_key_and_text(
    s3_client: Any,
    bucket: str,
    prefix: str,
    manifest_name: str,
) -> tuple[str, str]:
    clean_prefix = prefix.strip("/")
    primary_key = f"{clean_prefix}/{manifest_name}" if clean_prefix else manifest_name

    try:
        response = s3_client.get_object(Bucket=bucket, Key=primary_key)
        return primary_key, response["Body"].read().decode("utf-8-sig")
    except Exception:
        list_prefix = f"{clean_prefix}/" if clean_prefix else ""
        listed = s3_client.list_objects_v2(Bucket=bucket, Prefix=list_prefix)
        candidates = sorted(
            obj["Key"]
            for obj in listed.get("Contents", [])
            if obj.get("Key", "").lower().endswith(".sha256")
        )
        if not candidates:
            raise FileNotFoundError(
                f"No SHA-256 manifest found at s3://{bucket}/{primary_key} "
                f"or under prefix '{list_prefix}'."
            )
        preferred = [k for k in candidates if "manifest" in Path(k).name.lower()]
        chosen_key = preferred[0] if preferred else candidates[0]
        response = s3_client.get_object(Bucket=bucket, Key=chosen_key)
        return chosen_key, response["Body"].read().decode("utf-8-sig")


def _safe_extract_zip(archive_path: Path, extract_dir: Path) -> None:
    extract_root = extract_dir.resolve()
    with zipfile.ZipFile(archive_path, "r") as zf:
        for member in zf.infolist():
            target_path = (extract_root / member.filename).resolve()
            if target_path != extract_root and extract_root not in target_path.parents:
                raise ValueError(f"Blocked unsafe zip path traversal: {member.filename!r}")
        zf.extractall(extract_root)


def _safe_extract_tar(archive_path: Path, extract_dir: Path) -> None:
    extract_root = extract_dir.resolve()
    with tarfile.open(archive_path, "r:gz") as tf:
        members = tf.getmembers()
        for member in members:
            if member.issym() or member.islnk() or member.isdev():
                raise ValueError(f"Blocked unsafe link/device tar member: {member.name!r}")
            target_path = (extract_root / member.name).resolve()
            if target_path != extract_root and extract_root not in target_path.parents:
                raise ValueError(f"Blocked unsafe tar path traversal: {member.name!r}")
        tf.extractall(extract_root, members=members)


def _copy_extracted_payload(extract_dir: Path, dest: Path) -> None:
    source_root = extract_dir
    if (extract_dir / "landing" / "clients").is_dir():
        source_root = extract_dir / "landing"
    elif (extract_dir / "data" / "landing" / "clients").is_dir():
        source_root = extract_dir / "data" / "landing"

    dest.mkdir(parents=True, exist_ok=True)
    shutil.copytree(source_root, dest, dirs_exist_ok=True)

    outer_manifest = extract_dir / "MANIFEST.sha256"
    if (
        source_root != extract_dir
        and outer_manifest.is_file()
        and not (dest / "MANIFEST.sha256").is_file()
    ):
        shutil.copy2(outer_manifest, dest.parent / "MANIFEST.sha256")


def _extract_archive_to_dest(archive_path: Path, dest: Path, hint_name: str = "") -> None:
    name_lower = (hint_name or archive_path.name).lower()
    with tempfile.TemporaryDirectory(prefix="lakehouse_extract_") as tmp_extract:
        extract_dir = Path(tmp_extract)
        if name_lower.endswith((".tar.gz", ".tgz")):
            _safe_extract_tar(archive_path, extract_dir)
        elif name_lower.endswith(".zip") or zipfile.is_zipfile(archive_path):
            _safe_extract_zip(archive_path, extract_dir)
        else:
            _safe_extract_tar(archive_path, extract_dir)
        _copy_extracted_payload(extract_dir, dest)


def _is_archive_path(rel_path: str) -> bool:
    return rel_path.lower().endswith(_ARCHIVE_SUFFIXES)


def download_from_r2(
    *,
    account_id: str,
    bucket: str,
    prefix: str,
    access_key: str,
    secret_key: str,
    dest: Path,
    manifest_name: str = "MANIFEST.sha256",
    s3_client: Any | None = None,
) -> None:
    """Download and verify manifest-listed artifacts from Cloudflare R2 into `dest`."""
    client = s3_client or create_r2_client(account_id, access_key, secret_key)
    clean_prefix = prefix.strip("/")
    manifest_key, manifest_text = _discover_manifest_key_and_text(
        client, bucket=bucket, prefix=clean_prefix, manifest_name=manifest_name
    )
    print(f"Using manifest s3://{bucket}/{manifest_key}")
    entries = parse_sha256_manifest(manifest_text)

    with tempfile.TemporaryDirectory(prefix="lakehouse_r2_") as tmp_dir:
        tmp_root = Path(tmp_dir)
        downloaded_items: list[tuple[str, Path]] = []

        for expected_hash, rel_entry in entries:
            if clean_prefix and rel_entry.startswith(f"{clean_prefix}/"):
                object_key = rel_entry
                local_rel = rel_entry[len(clean_prefix) + 1 :]
            else:
                object_key = f"{clean_prefix}/{rel_entry}" if clean_prefix else rel_entry
                local_rel = rel_entry

            local_file = (tmp_root / local_rel).resolve()
            if tmp_root.resolve() not in local_file.parents:
                raise ValueError(f"Unsafe target path derived from manifest: {rel_entry!r}")
            local_file.parent.mkdir(parents=True, exist_ok=True)

            print(f"Downloading s3://{bucket}/{object_key}...")
            client.download_file(bucket, object_key, str(local_file))

            actual_hash = compute_sha256(local_file)
            if actual_hash != expected_hash:
                raise ValueError(
                    f"SHA-256 checksum mismatch for {object_key}: "
                    f"expected {expected_hash}, got {actual_hash}"
                )
            print(f"  [SHA-256 OK] {local_rel}")
            downloaded_items.append((local_rel, local_file))

        dest.mkdir(parents=True, exist_ok=True)
        for local_rel, local_file in downloaded_items:
            if _is_archive_path(local_rel):
                print(f"Extracting {local_rel} into {dest}...")
                _extract_archive_to_dest(local_file, dest, hint_name=local_rel)
            else:
                stripped_rel = local_rel
                for prefix_to_strip in ("data/landing/", "landing/"):
                    if stripped_rel.startswith(prefix_to_strip):
                        stripped_rel = stripped_rel[len(prefix_to_strip) :]
                        break
                dest_file = (dest.resolve() / stripped_rel).resolve()
                if dest.resolve() not in dest_file.parents:
                    raise ValueError(f"Unsafe destination path: {stripped_rel!r}")
                dest_file.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(local_file, dest_file)


def download_from_url(url: str, dest: Path) -> None:
    """Download and extract an archive from a direct HTTP/HTTPS/file URL."""
    clean_url = url.split("?", 1)[0].split("#", 1)[0]
    suffix = ".tar.gz" if clean_url.lower().endswith((".tar.gz", ".tgz")) else ".zip"
    with tempfile.TemporaryDirectory(prefix="lakehouse_http_") as tmp_dir:
        archive_path = Path(tmp_dir) / f"landing_dataset{suffix}"
        print(f"Downloading dataset archive from {url}...")
        urllib.request.urlretrieve(url, archive_path)
        print(f"Extracting archive into {dest}...")
        _extract_archive_to_dest(archive_path, dest, hint_name=clean_url)


def all_folders_populated(dest: Path) -> bool:
    """Return True if all 10 required dataset directories exist and contain >= 1 file."""
    for ds in REQUIRED_DATASETS:
        ds_dir = dest / ds
        if not ds_dir.is_dir():
            return False
        if not any(p.is_file() for p in ds_dir.rglob("*")):
            return False
    return True


def verify_local_manifest_if_present(dest: Path) -> None:
    """Verify SHA-256 checksums for an extracted local MANIFEST.sha256 if present."""
    candidates = [dest / "MANIFEST.sha256", dest.parent / "MANIFEST.sha256"]
    manifest_path = next((p for p in candidates if p.is_file()), None)
    if manifest_path is None:
        return

    entries = parse_sha256_manifest(manifest_path.read_text(encoding="utf-8-sig"))
    # If a parent-level manifest only tracks outer archive files (.zip/.tar.gz), skip local check.
    if manifest_path.parent != dest and all(_is_archive_path(rel) for _, rel in entries):
        return

    print(f"Verifying local checksums using {manifest_path}...")
    base_dir = manifest_path.parent.resolve()
    failures = 0
    for expected_hash, rel_path in entries:
        target = (base_dir / rel_path).resolve()
        if not target.is_file():
            print(f"  [FAIL] Missing file listed in manifest: {rel_path}", file=sys.stderr)
            failures += 1
            continue
        actual_hash = compute_sha256(target)
        if actual_hash != expected_hash:
            print(
                f"  [FAIL] Checksum mismatch for {rel_path}: "
                f"expected {expected_hash}, got {actual_hash}",
                file=sys.stderr,
            )
            failures += 1

    if failures:
        raise ValueError(f"Local manifest verification failed with {failures} error(s).")
    print("Local MANIFEST.sha256 checksums verified successfully.")


def verify_landing_folders(dest: Path) -> None:
    """Validate that all 10 required dataset folders exist and are non-empty."""
    if not dest.is_dir():
        raise RuntimeError(f"Destination directory does not exist: {dest}")

    missing_or_empty = 0
    print(f"\nValidating landing zone folders in {dest}:")
    for ds in REQUIRED_DATASETS:
        ds_dir = dest / ds
        if not ds_dir.is_dir():
            print(f"  [MISSING] {ds}/")
            missing_or_empty += 1
            continue
        file_count = sum(1 for p in ds_dir.rglob("*") if p.is_file())
        if file_count == 0:
            print(f"  [EMPTY]   {ds}/")
            missing_or_empty += 1
        else:
            print(f"  [OK]      {ds}/ ({file_count} files)")

    if missing_or_empty:
        raise RuntimeError(
            "Landing zone validation failed. Expected all 10 non-empty dataset folders."
        )
    print("\nSUCCESS: All 10 required datasets are present and verified.")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download and verify the CorpTech Enterprise Lakehouse landing dataset."
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to .env file with R2_* variables (default: .env).",
    )
    parser.add_argument(
        "--dest",
        default="./data/landing",
        help="Destination landing directory (default: ./data/landing).",
    )
    parser.add_argument("--r2-account-id", default=None, help="Cloudflare R2 account ID.")
    parser.add_argument("--r2-bucket", default=None, help="Cloudflare R2 bucket name.")
    parser.add_argument(
        "--r2-prefix",
        default=None,
        help="R2 prefix containing MANIFEST.sha256 (default: $R2_PREFIX or course-2026/archives).",
    )
    parser.add_argument("--r2-access-key", default=None, help="Cloudflare R2 access key ID.")
    parser.add_argument("--r2-secret-key", default=None, help="Cloudflare R2 secret access key.")
    parser.add_argument(
        "--manifest-name",
        default="MANIFEST.sha256",
        help="Manifest filename under R2 prefix (default: MANIFEST.sha256).",
    )
    parser.add_argument(
        "--url",
        default=None,
        help="Optional direct HTTP archive URL override (falls back to $DATASET_DOWNLOAD_URL).",
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Skip download and only verify existing destination folders and local manifest.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download even if all 10 landing folders already exist.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    load_dotenv_if_present(Path(args.env_file))
    dest = Path(args.dest)

    if not args.verify_only:
        if not args.force and all_folders_populated(dest):
            print(
                f"INFO: All 10 dataset folders already exist in {dest}. Use --force to re-download."
            )
        else:
            url = (
                args.url if args.url is not None else os.getenv("DATASET_DOWNLOAD_URL", "").strip()
            )
            account_id = (args.r2_account_id or os.getenv("R2_ACCOUNT_ID", "")).strip()
            bucket = (args.r2_bucket or os.getenv("R2_BUCKET_NAME", "")).strip()
            prefix = (
                args.r2_prefix
                if args.r2_prefix is not None
                else os.getenv("R2_PREFIX", "course-2026/archives")
            ).strip()
            access_key = (args.r2_access_key or os.getenv("R2_STUDENT_ACCESS_KEY_ID", "")).strip()
            secret_key = (
                args.r2_secret_key or os.getenv("R2_STUDENT_SECRET_ACCESS_KEY", "")
            ).strip()

            if args.url:
                download_from_url(args.url, dest)
            elif account_id or bucket or access_key or secret_key:
                missing_vars = [
                    name
                    for name, val in [
                        ("R2_ACCOUNT_ID", account_id),
                        ("R2_BUCKET_NAME", bucket),
                        ("R2_STUDENT_ACCESS_KEY_ID", access_key),
                        ("R2_STUDENT_SECRET_ACCESS_KEY", secret_key),
                    ]
                    if not val
                ]
                if missing_vars:
                    print(
                        "ERROR: Incomplete Cloudflare R2 configuration. Missing: "
                        + ", ".join(missing_vars),
                        file=sys.stderr,
                    )
                    return 1
                download_from_r2(
                    account_id=account_id,
                    bucket=bucket,
                    prefix=prefix,
                    access_key=access_key,
                    secret_key=secret_key,
                    dest=dest,
                    manifest_name=args.manifest_name,
                )
            elif url:
                download_from_url(url, dest)
            else:
                print(
                    "ERROR: No dataset download source configured.\n"
                    "Set R2_ACCOUNT_ID, R2_BUCKET_NAME, R2_PREFIX, "
                    "R2_STUDENT_ACCESS_KEY_ID, and R2_STUDENT_SECRET_ACCESS_KEY in .env "
                    "(or pass --url <URL> / --verify-only).",
                    file=sys.stderr,
                )
                return 1

    verify_local_manifest_if_present(dest)
    verify_landing_folders(dest)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
