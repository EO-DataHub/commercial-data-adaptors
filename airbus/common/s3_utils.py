import json
import logging
import os
import tarfile
import tempfile
import time

import boto3

s3 = boto3.client("s3")


class PollingTimeoutError(Exception):
    """Custom exception for polling timeout"""

    pass


def poll_s3_for_data(
    source_bucket: str, item_id: str, polling_interval: int = 60, timeout: int = 86400
) -> dict:
    """Poll the airbus S3 bucket for item_id and download the data"""
    start_time = time.time()
    end_time = start_time + timeout

    while True:
        # Check if the .tar.gz file exists in the source bucket
        logging.info(f"Checking for {item_id}.tar.gz file in bucket {source_bucket}...")
        response = s3.list_objects_v2(Bucket=source_bucket, Prefix=item_id)

        for obj in response.get("Contents", []):
            if obj["Key"].endswith(".tar.gz"):
                logging.info(f"File '{obj['Key']}' found in bucket '{source_bucket}'.")
                return obj

        # Check for timeout
        if time.time() > end_time:
            raise PollingTimeoutError(
                f"Timeout reached while polling for {item_id} in bucket {source_bucket} after {timeout} seconds."
            )

        # Wait for the specified interval before checking again
        time.sleep(polling_interval)


def unzip_and_upload_to_s3(
    source_bucket: str, destination_bucket: str, parent_folder: str, obj: dict
):
    """Unzip the contents of a .tar.gz file from S3 and upload them to a different S3 bucket"""
    # Create a temporary directory to store the downloaded and extracted files
    with tempfile.TemporaryDirectory() as tmpdir:
        # Download the .tar.gz file to the temporary directory
        tar_gz_path = os.path.join(tmpdir, os.path.basename(obj["Key"]))
        s3.download_file(source_bucket, obj["Key"], tar_gz_path)
        logging.info(
            f"Downloaded '{obj['Key']}' from bucket '{source_bucket}' to '{tar_gz_path}'."
        )

        # Extract the contents of the .tar.gz file
        with tarfile.open(tar_gz_path, "r:gz") as tar:
            tar.extractall(path=tmpdir)
            logging.info(f"Extracted '{obj['Key']}' to '{tmpdir}'.")

        # Upload the extracted files to the destination bucket
        for root, _, files in os.walk(tmpdir):
            for file in files:
                file_path = os.path.join(root, file)
                relative_path = os.path.relpath(file_path, tmpdir)
                s3_key = os.path.join(parent_folder, relative_path)
                s3.upload_file(file_path, destination_bucket, s3_key)
                logging.info(
                    f"Uploaded '{file_path}' to '{s3_key}' in bucket '{destination_bucket}'."
                )


def retrieve_stac_item(bucket: str, key: str) -> dict:
    """Retrieve a STAC item from an S3 bucket"""
    # Retrieve the STAC item from S3
    stac_item_obj = s3.get_object(Bucket=bucket, Key=key)
    stac_item = json.loads(stac_item_obj["Body"].read().decode("utf-8"))
    return stac_item


def list_objects_in_folder(bucket: str, folder_prefix: str) -> dict:
    """List objects in an S3 bucket with a specified folder prefix"""
    return s3.list_objects_v2(Bucket=bucket, Prefix=folder_prefix)


def upload_stac_item(bucket: str, key: str, stac_item: dict):
    """Upload a STAC item to an S3 bucket"""
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(stac_item))
    logging.info(f"Uploaded STAC item {key} to bucket {bucket}")
