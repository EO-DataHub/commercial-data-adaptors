import json
import time

import boto3

s3 = boto3.client("s3")


def poll_s3_for_data(
    source_bucket: str, item_id: str, polling_interval: int = 60
) -> dict:
    """Poll the airbus S3 bucket for item_id and download the data"""
    while True:
        # Check if the folder exists in the source bucket
        print(f"Checking for folder '{item_id}' in bucket '{source_bucket}'...")
        response = s3.list_objects_v2(Bucket=source_bucket, Prefix=f"{item_id}/")

        if "Contents" in response:
            print(f"Folder '{item_id}' found in bucket '{source_bucket}'.")
            return response

        # Wait for the specified interval before checking again
        time.sleep(polling_interval)


def move_data_to_workspace(
    source_bucket: str, destination_bucket: str, parent_folder: str, response: dict
):
    """Move all objects in the response to the destination bucket"""
    for obj in response["Contents"]:
        copy_source = {"Bucket": source_bucket, "Key": obj["Key"]}
        destination_key = f"{parent_folder}/{obj['Key']}"

        s3.copy_object(
            CopySource=copy_source, Bucket=destination_bucket, Key=destination_key
        )

        s3.delete_object(Bucket=source_bucket, Key=obj["Key"])
        print(
            f"Moved object '{obj['Key']}' to '{destination_key}' in bucket '{destination_bucket}'."
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
    print(f"Uploaded STAC item {key} to bucket {bucket}")
