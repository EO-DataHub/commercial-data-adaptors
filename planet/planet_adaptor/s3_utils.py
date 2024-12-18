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
        logging.info(f"Checking for {order_id} folder in bucket {source_bucket}...")
        response = s3_client.list_objects_v2(
            Bucket=source_bucket, Prefix=f"planet/{order_id}/"
        )
        for obj in response.get("Contents", []):
            if obj["Key"].endswith(f"{order_id}/manifest.json"):
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
    source_bucket: str,
    destination_bucket: str,
    parent_folder: str,
    order_id: str,
    item_id: str,
):
    """Unzip the contents of a .zip file from S3 and upload them to a different S3 bucket"""

    response = s3_client.list_objects_v2(
        Bucket=source_bucket, Prefix=f"planet/{order_id}"
    )

    for obj in response.get("Contents", []):
        logging.info(f"File '{obj['Key']}' found in bucket '{source_bucket}'.")

        if obj["Key"].endswith(".zip"):
            # Create a temporary directory to store the downloaded and extracted files
            with tempfile.TemporaryDirectory() as tmpdir:
                # Download the .zip file to the temporary directory
                zip_path = os.path.join(tmpdir, os.path.basename(obj["Key"]))
                s3_client.download_file(source_bucket, obj["Key"], zip_path)
                logging.info(
                    f"Downloaded '{obj['Key']}' from bucket '{source_bucket}' to '{zip_path}'."
                )

                # Rename zip file with item ID instead of order ID
                try:
                    new_zip_path = zip_path.replace(order_id, item_id)
                    os.replace(zip_path, new_zip_path)
                except OSError:
                    new_zip_path = zip_path

                # Extract the contents of the .zip file
                with zipfile.ZipFile(new_zip_path) as z:
                    z.extractall(tmpdir)
                    logging.info(f"Extracted '{obj['Key']}' to '{tmpdir}'.")

                # Upload the extracted files to the destination bucket
                for root, _, files in os.walk(tmpdir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        relative_path = os.path.relpath(file_path, tmpdir)
                        s3_key = os.path.join(parent_folder, relative_path)
                        s3_client.upload_file(file_path, destination_bucket, s3_key)
                        logging.info(
                            f"Uploaded '{file_path}' to '{s3_key}' in bucket '{destination_bucket}'."
                        )
        else:
            dest_file_path = f"{parent_folder.rsplit('/', 1)[0]}/{item_id}/{obj['Key']}"
            source = {"Bucket": source_bucket, "Key": obj["Key"]}
            dest = s3_resource.Bucket(destination_bucket)
            dest.copy(source, dest_file_path)
            file_name = obj["Key"]
            logging.info(
                f"Uploaded '{file_name}' to '{dest_file_path}' in bucket '{destination_bucket}'."
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


def upload_stac_item(bucket: str, key: str, stac_item: dict):
    """Upload a STAC item to an S3 bucket"""
    s3_client.put_object(Bucket=bucket, Key=key, Body=json.dumps(stac_item))
    logging.info(f"Uploaded STAC item {key} to bucket {bucket}")
