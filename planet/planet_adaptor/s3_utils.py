import json
import logging
import os
import time
import zipfile

import boto3

s3_client = boto3.client("s3")


class PollingTimeoutError(Exception):
    """Custom exception for polling timeout"""

    pass


def poll_s3_for_data(
    source_bucket: str,
    order_id: str,
    polling_interval: int = 60,
    timeout: int = 86400,
) -> dict:
    """Poll the planet S3 bucket for item_id and download the data"""
    start_time = time.time()
    end_time = start_time + timeout

    while True:
        # Check if the folder containing the order exists in the source bucket
        folder = f"planet/commercial-data/{order_id}/"
        logging.info(f"Checking for {folder} folder in bucket {source_bucket}...")
        response = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=folder)
        for obj in response.get("Contents", []):
            if obj["Key"].endswith(
                f"{order_id}/manifest.json"
            ):  # manifest.json is the final file to be delivered
                logging.info(
                    f"Data available: file '{obj['Key']}' found in bucket '{source_bucket}'."
                )
                return obj

        # Check for timeout
        if time.time() > end_time:
            raise PollingTimeoutError(
                f"Timeout reached while polling for {order_id} in bucket {source_bucket} after {timeout} seconds."
            )

        # Wait for the specified interval before checking again
        time.sleep(polling_interval)


def download_and_store_locally(
    source_bucket: str, parent_folder: str, destination_folder: str
):
    """Unzip the contents of a .zip file from S3 and store them locally in a specified folder"""
    # Create the destination folder if it doesn't exist
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)

    response = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=parent_folder)

    for obj in response.get("Contents", []):
        logging.info(f"File '{obj['Key']}' found in bucket '{source_bucket}'.")
        destination_file_path = os.path.join(
            destination_folder, os.path.basename(obj["Key"])
        )
        s3_client.download_file(source_bucket, obj["Key"], destination_file_path)
        logging.info(
            f"Downloaded '{obj['Key']}' from bucket '{source_bucket}' to '{destination_file_path}'."
        )

        if obj["Key"].endswith(".zip"):
            logging.info("Zip file found. Unzipping...")

            # Extract the contents of the .zip file
            with zipfile.ZipFile(destination_file_path) as z:
                z.extractall(path=destination_folder)
                logging.info(f"Extracted '{obj['Key']}' to '{destination_folder}'.")


def retrieve_stac_item(file_path: str) -> dict:
    """Retrieve a STAC item from a local JSON file"""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file {file_path} does not exist.")

    with open(file_path, "r", encoding="utf-8") as f:
        stac_item = json.load(f)
    return stac_item
