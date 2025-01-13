import json
import logging
import os
import tempfile
import time
import zipfile

import boto3

s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")


class PollingTimeoutError(Exception):
    """Custom exception for polling timeout"""

    pass


def poll_s3_for_data(
    source_bucket: str,
    order_id: str,
    item_id: str,
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
                f"Timeout reached while polling for {item_id} in bucket {source_bucket} after {timeout} seconds."
            )

        # Wait for the specified interval before checking again
        time.sleep(polling_interval)


def download_data(
    bucket: str,
    key: str,
    file_name: str,
) -> str:
    """Download the data and save locally"""

    logging.info(f"Downloading from {bucket}/{key} and saving as {file_name}")
    s3_client.download_file(bucket, key, file_name)

    return file_name


def unzip_and_upload_to_s3(
    bucket: str,
    parent_folder: str,
    order_id: str,
    item_id: str,
) -> None:
    """Unzip the contents of a .zip file from S3 and upload them"""

    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=parent_folder)

    for obj in response.get("Contents", []):
        logging.info(f"File '{obj['Key']}' found in bucket '{bucket}'.")

        if obj["Key"].endswith(".zip"):
            # Create a temporary directory to store the downloaded and extracted files
            logging.info("Zip file found. Unzipping...")
            with tempfile.TemporaryDirectory() as tmpdir:
                # Download the .zip file to the temporary directory
                zip_path = os.path.join(tmpdir, os.path.basename(obj["Key"]))
                s3_client.download_file(bucket, obj["Key"], zip_path)
                logging.info(
                    f"Downloaded '{obj['Key']}' from bucket '{bucket}' to '{zip_path}'."
                )

                # Extract the contents of the .zip file
                with zipfile.ZipFile(zip_path) as z:
                    z.extractall(tmpdir)
                    logging.info(f"Extracted '{obj['Key']}' to '{tmpdir}'.")

                # Upload the extracted files to the destination bucket
                for root, _, files in os.walk(tmpdir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        relative_path = os.path.relpath(file_path, tmpdir)
                        s3_key = os.path.join(item_id, relative_path)

                        s3_client.upload_file(file_path, bucket, f"planet/{s3_key}")
                        logging.info(
                            f"Uploaded '{file_path}' to '{s3_key}' in bucket '{bucket}'."
                        )
        else:
            dest_file_path = (
                f"planet/{item_id}/{obj['Key'].replace(parent_folder + '/', '')}"
            )
            source = {"Bucket": bucket, "Key": obj["Key"]}
            dest = s3_resource.Bucket(bucket)
            dest.copy(source, dest_file_path)
            file_name = obj["Key"]
            logging.info(
                f"Uploaded '{file_name}' to '{dest_file_path}' in bucket '{bucket}'."
            )


def retrieve_stac_item(bucket: str, key: str) -> dict:
    """Retrieve a STAC item from an S3 bucket"""
    # Retrieve the STAC item from S3
    stac_item_obj = s3_client.get_object(Bucket=bucket, Key=key)
    stac_item = json.loads(stac_item_obj["Body"].read().decode("utf-8"))
    return stac_item


def list_objects_in_folder(bucket: str, folder_prefix: str) -> dict:
    """List objects in an S3 bucket with a specified folder prefix"""
    return s3_client.list_objects_v2(Bucket=bucket, Prefix=folder_prefix)


def upload_stac_item(bucket: str, key: str, stac_item: dict) -> None:
    """Upload a STAC item to an S3 bucket"""
    s3_client.put_object(Bucket=bucket, Key=key, Body=json.dumps(stac_item))
    logging.info(f"Uploaded STAC item {key} to bucket {bucket}")
