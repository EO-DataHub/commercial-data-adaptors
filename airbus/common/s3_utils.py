import json
import logging
import os
import tarfile
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


def download_and_store_locally(source_bucket: str, obj: dict, destination_folder: str):
    """Unzip the contents of a .tar.gz file from S3 and store them locally in a specified folder"""
    # Create the destination folder if it doesn't exist
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)

    # Download the .tar.gz file to the destination folder
    tar_gz_path = os.path.join(destination_folder, os.path.basename(obj["Key"]))
    s3.download_file(source_bucket, obj["Key"], tar_gz_path)
    logging.info(
        f"Downloaded '{obj['Key']}' from bucket '{source_bucket}' to '{tar_gz_path}'."
    )

    # Extract the contents of the .tar.gz file into the destination folder
    with tarfile.open(tar_gz_path, "r:gz") as tar:
        tar.extractall(path=destination_folder)
        logging.info(f"Extracted '{tar_gz_path}' to '{destination_folder}'.")


def list_objects_in_folder(bucket: str, folder_prefix: str) -> dict:
    """List objects in an S3 bucket with a specified folder prefix"""
    return s3.list_objects_v2(Bucket=bucket, Prefix=folder_prefix)


def upload_stac_item(bucket: str, key: str, stac_item: dict):
    """Upload a STAC item to an S3 bucket"""
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(stac_item))
    logging.info(f"Uploaded STAC item {key} to bucket {bucket}")
