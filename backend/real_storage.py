"""
Real S3-compatible storage layer using moto[s3].

moto provides a real HTTP server that speaks the S3 protocol.
In production, replace with MinIO, Ozone S3 Gateway, or AWS S3.

Start the S3 server:
    moto_server s3 -p 5001

Then connect with boto3 using endpoint_url="http://localhost:5001"
"""

import os
import json
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


@dataclass
class S3StorageConfig:
    """Configuration for S3-compatible storage."""
    endpoint_url: str = "http://localhost:5001"
    region_name: str = "us-east-1"
    access_key: str = "fake-access-key"
    secret_key: str = "fake-secret-key"
    bucket_name: str = "lakehouse-demo"


class LakehouseS3Storage:
    """
    Real S3-compatible storage using boto3 against moto/MinIO/Ozone.

    Bucket layout:
        lakehouse-demo/
        ├── raw_audio/{call_id}.json           # raw transcripts
        ├── voice_analysis/{call_id}.json      # parsed results (Lance dataset)
        └── analytics/{yyyy-mm-dd}.parquet     # daily aggregations (Iceberg table)
    """

    def __init__(self, config: Optional[S3StorageConfig] = None):
        self.config = config or S3StorageConfig()
        self.client = boto3.client(
            "s3",
            endpoint_url=self.config.endpoint_url,
            region_name=self.config.region_name,
            aws_access_key_id=self.config.access_key,
            aws_secret_access_key=self.config.secret_key,
            config=Config(
                signature_version="s3v4",
                retries={"max_attempts": 1},
            ),
        )
        self._ensure_bucket()

    def _ensure_bucket(self):
        """Create bucket if it doesn't exist."""
        try:
            self.client.head_bucket(Bucket=self.config.bucket_name)
        except ClientError:
            try:
                self.client.create_bucket(
                    Bucket=self.config.bucket_name,
                    CreateBucketConfiguration={
                        "LocationConstraint": self.config.region_name
                    },
                )
            except ClientError:
                # Fallback: create without LocationConstraint (moto compatibility)
                self.client.create_bucket(
                    Bucket=self.config.bucket_name,
                )

    # ── Raw audio (transcript) storage ──

    def save_raw_transcript(self, call_id: str, transcript: str,
                            metadata: Optional[Dict] = None) -> str:
        """Save raw call transcript to S3."""
        key = f"raw_audio/{call_id}.json"
        data = {
            "call_id": call_id,
            "transcript": transcript,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "metadata": metadata or {},
        }
        self.client.put_object(
            Bucket=self.config.bucket_name,
            Key=key,
            Body=json.dumps(data, ensure_ascii=False, indent=2),
            ContentType="application/json",
        )
        return key

    def get_raw_transcript(self, call_id: str) -> Optional[Dict]:
        """Retrieve raw transcript from S3."""
        try:
            resp = self.client.get_object(
                Bucket=self.config.bucket_name,
                Key=f"raw_audio/{call_id}.json",
            )
            return json.loads(resp["Body"].read().decode("utf-8"))
        except ClientError:
            return None

    # ── Voice analysis results (Lance dataset equivalent) ──

    def save_analysis_result(self, call_id: str, analysis: Dict) -> str:
        """Save parsed analysis to S3 (mirrors Lance dataset write)."""
        key = f"voice_analysis/{call_id}.json"
        self.client.put_object(
            Bucket=self.config.bucket_name,
            Key=key,
            Body=json.dumps(analysis, ensure_ascii=False, indent=2),
            ContentType="application/json",
        )
        return key

    def get_analysis_result(self, call_id: str) -> Optional[Dict]:
        """Retrieve analysis from S3."""
        try:
            resp = self.client.get_object(
                Bucket=self.config.bucket_name,
                Key=f"voice_analysis/{call_id}.json",
            )
            return json.loads(resp["Body"].read().decode("utf-8"))
        except ClientError:
            return None

    # ── Analytics / Iceberg table equivalent ──

    def save_daily_aggregation(self, date_str: str, data: Dict) -> str:
        """Save daily aggregation to S3 (mirrors Iceberg table write)."""
        key = f"analytics/{date_str}.json"
        self.client.put_object(
            Bucket=self.config.bucket_name,
            Key=key,
            Body=json.dumps(data, ensure_ascii=False, indent=2),
            ContentType="application/json",
        )
        return key

    def get_daily_aggregation(self, date_str: str) -> Optional[Dict]:
        """Retrieve daily aggregation from S3."""
        try:
            resp = self.client.get_object(
                Bucket=self.config.bucket_name,
                Key=f"analytics/{date_str}.json",
            )
            return json.loads(resp["Body"].read().decode("utf-8"))
        except ClientError:
            return None

    # ── List operations ──

    def list_objects(self, prefix: str) -> List[Dict]:
        """List objects under a prefix."""
        try:
            resp = self.client.list_objects_v2(
                Bucket=self.config.bucket_name,
                Prefix=prefix,
            )
            return [
                {
                    "key": obj["Key"],
                    "size": obj["Size"],
                    "last_modified": obj["LastModified"].isoformat(),
                }
                for obj in resp.get("Contents", [])
            ]
        except ClientError:
            return []

    def get_storage_stats(self) -> Dict[str, Any]:
        """Get storage statistics for the demo platform."""
        raw_count = len(self.list_objects("raw_audio/"))
        analysis_count = len(self.list_objects("voice_analysis/"))
        analytics_count = len(self.list_objects("analytics/"))

        return {
            "endpoint": self.config.endpoint_url,
            "bucket": self.config.bucket_name,
            "total_objects": raw_count + analysis_count + analytics_count,
            "raw_audio_count": raw_count,
            "voice_analysis_count": analysis_count,
            "analytics_count": analytics_count,
            "storage_type": "S3-compatible (moto/MinIO/Ozone)",
        }
