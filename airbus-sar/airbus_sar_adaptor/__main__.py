import json
import logging
import mimetypes
import sys
from enum import Enum

from airbus_sar_adaptor.api_utils import post_submit_order
from airbus_sar_adaptor.s3_utils import (
    list_objects_in_folder,
    move_data_to_workspace,
    poll_s3_for_data,
    retrieve_stac_item,
    upload_stac_item,
)
from airbus_sar_adaptor.stac_utils import (
    get_acquisition_id_from_stac,
    update_stac_order_status,
    write_stac_item_and_catalog,
)
from pulsar import Client as PulsarClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class OrderStatus(Enum):
    ORDERABLE = "orderable"
    ORDERED = "ordered"
    PENDING = "pending"
    SHIPPING = "shipping"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELED = "canceled"


def send_pulsar_message(bucket: str, key: str):
    """Send a Pulsar message to indicate an update to the item"""
    pulsar_client = PulsarClient("pulsar://pulsar-broker.pulsar:6650")
    producer = pulsar_client.create_producer(
        topic="harvested",
        producer_name="resource_catalogue_fastapi",
        chunking_enabled=True,
    )
    parts = key.split("/")
    workspace = parts[0]
    file_id = parts[-1]
    output_data = {
        "id": f"{workspace}/order_item/{file_id}",
        "workspace": workspace,
        "bucket_name": bucket,
        "added_keys": [],
        "updated_keys": [key],
        "deleted_keys": [],
        "source": workspace,
        "target": f"user-datasets/{workspace}",
    }
    logging.info(f"Sending message to pulsar: {output_data}")
    producer.send((json.dumps(output_data)).encode("utf-8"))


def update_stac_item_success(bucket: str, key: str, parent_folder: str, item_id: str):
    """Update the STAC item with the assets and success order status"""
    stac_item = retrieve_stac_item(bucket, key)
    # List files and folders in the specified folder
    folder_prefix = f"{parent_folder}/{item_id}/"
    folder_objects = list_objects_in_folder(bucket, folder_prefix)

    # Add all listed objects as assets to the STAC item
    if "Contents" in folder_objects:
        for obj in folder_objects["Contents"]:
            file_key = obj["Key"]

            # Skip if the file_key is a folder
            if file_key.endswith("/"):
                continue

            asset_name = file_key.split("/")[-1]

            # Determine the MIME type of the file
            mime_type, _ = mimetypes.guess_type(file_key)
            if mime_type is None:
                mime_type = "application/octet-stream"  # Default MIME type

            # Add asset link to the file
            stac_item["assets"][asset_name] = {
                "href": f"s3://{bucket}/{file_key}",
                "type": mime_type,
            }
    # Mark the order as succeeded and upload the updated STAC item
    update_stac_order_status(stac_item, item_id, OrderStatus.SUCCEEDED.value)
    upload_stac_item(bucket, key, stac_item)
    send_pulsar_message(bucket, key)

    # Create local record of the order, to be used as the workflow output
    write_stac_item_and_catalog(stac_item, key.split("/")[-1], item_id)


def update_stac_item_failure(bucket: str, key: str, item_id: str):
    """Update the STAC item with the failure order status"""
    stac_item = retrieve_stac_item(bucket, key)

    # Mark the order as failed and upload the updated STAC item
    update_stac_order_status(stac_item, item_id, OrderStatus.FAILED.value)
    upload_stac_item(bucket, key, stac_item)
    send_pulsar_message(bucket, key)

    # Create local record of attempted order, to be used as the workflow output
    write_stac_item_and_catalog(stac_item, key.split("/")[-1], item_id)


def main(stac_key: str, workspace_bucket: str):
    """Submit an order for an acquisition, retrieve the data, and update the STAC item"""
    # Workspace STAC item should already be generated and ingested, with an order status of ordered.
    stac_parent_folder = "/".join(stac_key.split("/")[:-1])
    try:
        # Submit an order for the given STAC item
        stac_item = retrieve_stac_item(workspace_bucket, stac_key)
        acquisition_id = get_acquisition_id_from_stac(stac_item, stac_key)
        item_id = post_submit_order(acquisition_id)
    except Exception as e:
        logging.error(f"Failed to submit order: {e}")
        update_stac_item_failure(workspace_bucket, stac_key, None)
        return
    try:
        # Wait for data from airbus to arrive, then move it to the workspace
        response = poll_s3_for_data("commercial-data-airbus", item_id)
        move_data_to_workspace(
            "commercial-data-airbus", workspace_bucket, stac_parent_folder, response
        )
    except Exception as e:
        logging.error(f"Failed to retrieve data: {e}")
        update_stac_item_failure(workspace_bucket, stac_key, item_id)
        return
    update_stac_item_success(workspace_bucket, stac_key, stac_parent_folder, item_id)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
