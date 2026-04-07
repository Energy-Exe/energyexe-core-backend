"""Generic AWS S3 file storage service.

Provides upload/download/delete for any file type. Callers are responsible
for building S3 keys and enforcing access control.

Key conventions by feature:
  brain-agent/{user_id}/{thread_id}/{filename}  — agent-generated files
  (add more prefixes as other features adopt S3)
"""

import asyncio
import mimetypes
from pathlib import Path
from typing import Optional

import structlog

from app.core.config import get_settings

logger = structlog.get_logger(__name__)

_s3_client = None


def _get_s3_client():
    """Get or create a singleton boto3 S3 client."""
    global _s3_client
    if _s3_client is None:
        import boto3

        settings = get_settings()
        _s3_client = boto3.client("s3", region_name=settings.S3_REGION)
    return _s3_client


def guess_content_type(filename: str) -> str:
    """Guess MIME type from filename, falling back to octet-stream."""
    ct, _ = mimetypes.guess_type(filename)
    return ct or "application/octet-stream"


async def upload_file(key: str, file_path: Path, content_type: Optional[str] = None) -> str:
    """Upload a file to S3. Returns the key on success, empty string if S3 is disabled."""
    settings = get_settings()
    if not settings.s3_enabled:
        return ""

    ct = content_type or guess_content_type(file_path.name)

    def _upload():
        client = _get_s3_client()
        client.upload_file(
            str(file_path),
            settings.S3_BUCKET_NAME,
            key,
            ExtraArgs={"ContentType": ct},
        )
        return key

    result = await asyncio.to_thread(_upload)
    logger.info("s3_file_uploaded", key=result)
    return result


async def download_file(key: str) -> Optional[bytes]:
    """Download a file from S3. Returns bytes or None if not found."""
    settings = get_settings()
    if not settings.s3_enabled:
        return None

    def _download():
        from botocore.exceptions import ClientError

        client = _get_s3_client()
        try:
            response = client.get_object(Bucket=settings.S3_BUCKET_NAME, Key=key)
            return response["Body"].read()
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return None
            raise

    return await asyncio.to_thread(_download)


async def delete_by_prefix(prefix: str) -> int:
    """Delete all objects under a prefix. Returns count deleted."""
    settings = get_settings()
    if not settings.s3_enabled:
        return 0

    def _delete():
        client = _get_s3_client()
        paginator = client.get_paginator("list_objects_v2")
        deleted = 0
        for page in paginator.paginate(Bucket=settings.S3_BUCKET_NAME, Prefix=prefix):
            objects = page.get("Contents", [])
            if objects:
                client.delete_objects(
                    Bucket=settings.S3_BUCKET_NAME,
                    Delete={"Objects": [{"Key": obj["Key"]} for obj in objects]},
                )
                deleted += len(objects)
        return deleted

    count = await asyncio.to_thread(_delete)
    if count:
        logger.info("s3_prefix_deleted", prefix=prefix, count=count)
    return count
