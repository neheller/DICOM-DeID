#!/usr/bin/env python3
"""
Upload all files in a local folder to an S3 URI like s3://bucket/prefix

Examples:
  python upload_folder_to_s3.py /path/to/folder s3://my-bucket/backups/projectX/
"""

import argparse
import concurrent.futures
import mimetypes
import os
from pathlib import Path, PurePosixPath
import sys
import threading
from urllib.parse import urlparse

import boto3
from botocore.exceptions import BotoCoreError, ClientError


def parse_s3_uri(s3_uri: str):
    """
    Parse an S3 URI into (bucket, prefix). Adds no leading slash to prefix.
    """
    if not s3_uri.startswith("s3://"):
        raise ValueError("S3 path must start with s3://")
    parsed = urlparse(s3_uri)
    bucket = parsed.netloc
    # Remove any leading slash from path and keep trailing slash if given
    prefix = parsed.path.lstrip("/")
    return bucket, prefix


def iter_files(root: Path):
    """
    Yield all files under root.
    """
    for p in root.rglob("*"):
        if p.is_file():
            yield p


class ProgressPrinter:
    """
    Thread safe progress reporter for multipart uploads.
    """

    def __init__(self, total_bytes: int):
        self._total = total_bytes
        self._seen = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen += bytes_amount
            if self._total > 0:
                pct = (self._seen / self._total) * 100
                # Print on one line
                print(f"\rUploaded {self._seen:,} of {self._total:,} bytes ({pct:5.1f}%)", end="", flush=True)


def build_s3_key(local_file: Path, local_root: Path, prefix: str) -> str:
    """
    Build an S3 key that preserves the relative path under the given prefix.
    Always uses forward slashes.
    """
    rel = local_file.relative_to(local_root)
    rel_posix = PurePosixPath(*rel.parts)  # convert Windows backslashes to forward slashes
    if prefix and not prefix.endswith("/"):
        prefix = prefix + "/"
    return f"{prefix}{rel_posix}" if prefix else str(rel_posix)


def guess_extra_args(local_file: Path):
    """
    Set helpful metadata like ContentType based on file extension.
    """
    ctype, _ = mimetypes.guess_type(str(local_file))
    extra = {}
    if ctype:
        extra["ContentType"] = ctype
    return extra


def upload_one(s3_client, bucket: str, key: str, local_file: Path, extra_args: dict, progress_cb):
    try:
        s3_client.upload_file(
            Filename=str(local_file),
            Bucket=bucket,
            Key=key,
            ExtraArgs=extra_args,
            Callback=progress_cb
        )
        return key, None
    except (BotoCoreError, ClientError) as e:
        return key, e


def main():
    parser = argparse.ArgumentParser(description="Upload all files in a folder to an S3 path.")
    parser.add_argument("local_folder", type=str, help="Path to local folder to upload")
    parser.add_argument("s3_path", type=str, help="Target like s3://bucket/prefix/")
    parser.add_argument("--workers", type=int, default=8, help="Number of parallel uploads")
    parser.add_argument("--acl", type=str, default=None, help="Optional Canned ACL like private or public-read")
    parser.add_argument("--storage-class", type=str, default=None, help="Optional storage class like STANDARD_IA")
    parser.add_argument("--dry-run", action="store_true", help="List files and keys without uploading")
    args = parser.parse_args()

    local_root = Path(args.local_folder).expanduser().resolve()
    if not local_root.exists() or not local_root.is_dir():
        print(f"Local folder does not exist or is not a directory: {local_root}", file=sys.stderr)
        sys.exit(2)

    bucket, prefix = parse_s3_uri(args.s3_path)

    all_files = list(iter_files(local_root))
    if not all_files:
        print("No files found to upload. Exiting.")
        return

    total_bytes = sum(p.stat().st_size for p in all_files)
    progress = ProgressPrinter(total_bytes)

    if args.dry_run:
        print("Dry run. Files that would be uploaded:")
        for f in all_files:
            key = build_s3_key(f, local_root, prefix)
            print(f"{f}  ->  s3://{bucket}/{key}")
        print(f"Total files: {len(all_files)}")
        print(f"Total bytes: {total_bytes:,}")
        return

    session = boto3.session.Session()
    s3_client = session.client("s3")

    # Prepare shared ExtraArgs template
    base_extra = {}
    if args.acl:
        base_extra["ACL"] = args.acl
    if args.storage_class:
        base_extra["StorageClass"] = args.storage_class

    print(f"Uploading {len(all_files)} files from {local_root} to s3://{bucket}/{prefix}")
    print("Starting uploads...")

    results = []
    errors = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = []
        for f in all_files:
            key = build_s3_key(f, local_root, prefix)
            extra = {**base_extra, **guess_extra_args(f)}
            futures.append(
                executor.submit(upload_one, s3_client, bucket, key, f, extra, progress)
            )

        for fut in concurrent.futures.as_completed(futures):
            key, err = fut.result()
            if err is None:
                results.append(key)
            else:
                errors.append((key, err))

    # Finish the progress line
    print()

    print(f"Uploaded {len(results)} files.")
    if errors:
        print(f"{len(errors)} uploads failed. Listing failures:", file=sys.stderr)
        for key, err in errors:
            print(f"- s3://{bucket}/{key}  error: {err}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
