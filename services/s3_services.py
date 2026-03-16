"""
S3 Service
==========
Handles uploading, downloading, and listing from AWS S3.
"""

import boto3
import os
import logging
from botocore.exceptions import ClientError
from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION_NAME, S3_BUCKET_NAME

logger = logging.getLogger(__name__)

class S3Service:
    _client = None

    @classmethod
    def get_client(cls):
        if cls._client is None:
            if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
                cls._client = boto3.client(
                    's3',
                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                    region_name=AWS_REGION_NAME
                )
            else:
                # Fallback if credentials are automatically provided via IAM role or ~/.aws/credentials
                cls._client = boto3.client('s3', region_name=AWS_REGION_NAME)
        return cls._client

    @classmethod
    def upload_file(cls, file_path: str, s3_key: str) -> bool:
        """Upload a file to the configured S3 bucket"""
        if not os.path.isfile(file_path):
            logger.error("S3 Upload Failed: File not found %s", file_path)
            return False
            
        try:
            client = cls.get_client()
            client.upload_file(file_path, S3_BUCKET_NAME, s3_key)
            logger.info(f"✓ Uploaded: {file_path} → s3://{S3_BUCKET_NAME}/{s3_key}")
            return True
        except ClientError as e:
            logger.error(f"✗ AWS S3 Upload Error: {e}")
            return False
        except Exception as e:
            logger.error(f"✗ Unexpected S3 Upload Error: {e}")
            return False

    @classmethod
    def access_file(cls, s3_key: str, download_path: str) -> bool:
        """Download/access a file from the configured S3 bucket"""
        # Ensure parent directory of download_path exists
        os.makedirs(os.path.dirname(download_path), exist_ok=True)
        
        try:
            client = cls.get_client()
            client.download_file(S3_BUCKET_NAME, s3_key, download_path)
            logger.info(f"✓ Downloaded: s3://{S3_BUCKET_NAME}/{s3_key} → {download_path}")
            return True
        except ClientError as e:
            logger.error(f"✗ AWS S3 Download Error: {e}")
            return False
        except Exception as e:
            logger.error(f"✗ Unexpected S3 Download Error: {e}")
            return False
            
    @classmethod
    def get_bucket_names(cls) -> list:
        """Fetch all S3 bucket names (utility)"""
        try:
            client = cls.get_client()
            response = client.list_buckets()
            buckets = [bucket['Name'] for bucket in response['Buckets']]
            return buckets
        except ClientError as e:
            logger.error(f"✗ AWS S3 List Buckets Error: {e}")
            return []
        except Exception as e:
            logger.error(f"✗ Unexpected S3 List Buckets Error: {e}")
            return []

    @classmethod
    def folder_upload_artifacts(cls, local_dir: str, s3_prefix: str) -> None:
        """Uploads all files in a directory to an S3 prefix."""
        for root, dirs, files in os.walk(local_dir):
            for file in files:
                local_path = os.path.join(root, file)
                # Ensure the S3 key structure mirrors the local relative structure
                rel_path = os.path.relpath(local_path, local_dir)
                # Fix path separators to be / for S3
                s3_key = f"{s3_prefix}/{rel_path}".replace(os.sep, '/')
                cls.upload_file(local_path, s3_key)
