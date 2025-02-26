import logging
import os
import tarfile
import time
import zipfile

import boto3

s3 = boto3.client("s3")


class PollingTimeoutError(Exception):
    """Custom exception for polling timeout"""

    pass


def poll_s3_for_data(
    source_bucket: str, item_prefix: str, item_suffix: str, polling_interval: int = 60, timeout: int = 86400
) -> dict:
    """Poll an S3 bucket for an item with given prefix and suffix, and return the object details"""
    start_time = time.time()
    end_time = start_time + timeout

    while True:
        # Check if the file exists in the source bucket
        logging.info(f"Checking for item with prefix {item_prefix} and suffix {item_suffix} file in bucket {source_bucket}...")
        response = s3.list_objects_v2(Bucket=source_bucket, Prefix=item_prefix)

        for obj in response.get("Contents", []):
            if obj["Key"].endswith(item_suffix):
                logging.info(f"File '{obj['Key']}' found in bucket '{source_bucket}'.")
                return obj

        # Check for timeout
        if time.time() > end_time:
            raise PollingTimeoutError(
                f"Timeout reached while polling for item in bucket {source_bucket} after {timeout} seconds."
            )

        # Wait for the specified interval before checking again
        time.sleep(polling_interval)


def download_and_store_locally(source_bucket: str, obj: dict, destination_folder: str):
    """Unzip the contents of an archive file from S3 and store them locally in a specified folder"""
    # Create the destination folder if it doesn't exist
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)

    # Download the archive to the destination folder
    local_archive_path = os.path.join(destination_folder, os.path.basename(obj["Key"]))
    s3.download_file(source_bucket, obj["Key"], local_archive_path)
    logging.info(
        f"Downloaded '{obj['Key']}' from bucket '{source_bucket}' to '{local_archive_path}'."
    )

    if local_archive_path.endswith(".tar.gz"):
        # Extract the contents of the .tar.gz file into the destination folder
        with tarfile.open(local_archive_path, "r:gz") as tar:
            tar.extractall(path=destination_folder)
            logging.info(f"Extracted '{local_archive_path}' to '{destination_folder}'.")
    elif local_archive_path.endswith(".zip"):
        # Extract the contents of the .zip file into the destination folder
        with zipfile.ZipFile(local_archive_path, "r") as zip_ref:
            zip_ref.extractall(destination_folder)
            logging.info(f"Extracted '{local_archive_path}' to '{destination_folder}'.")
    else:
        logging.warning(f"Unsupported file format for '{local_archive_path}'. Skipping extraction.")
