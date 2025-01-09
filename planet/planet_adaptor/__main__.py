import asyncio
import glob
import json
import logging
import mimetypes
import os
import sys
from enum import Enum

from planet_adaptor.api_utils import (
    create_order_request,
    define_delivery,
    get_api_key_from_secret,
    submit_order,
)
from planet_adaptor.s3_utils import (
    download_data,
    list_objects_in_folder,
    poll_s3_for_data,
    retrieve_stac_item,
    unzip_and_upload_to_s3,
    upload_stac_item,
)
from planet_adaptor.stac_utils import (
    get_id_and_collection_from_stac,
    update_stac_order_status,
    write_stac_item_and_catalog,
)
from pulsar import Client as PulsarClient

import planet

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
    CANCELED = "canceled"
    FAILED = "failed"


def send_pulsar_message(bucket: str, key: str):
    """Send a Pulsar message to indicate an update to the item"""
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
    pulsar_client = PulsarClient("pulsar://pulsar-broker.pulsar:6650")
    producer = pulsar_client.create_producer(
        topic="harvested",
        producer_name=f"planet-adaptor-{workspace}-{file_id}",
        chunking_enabled=True,
    )
    producer.send((json.dumps(output_data)).encode("utf-8"))


def update_stac_item_success(
    bucket: str, key: str, folder: str, item_id: str, workspaces_domain: str
):
    """Update the STAC item with the assets and success order status"""
    stac_item = retrieve_stac_item(bucket, key)
    stac_item["assets"] = {}
    # List files and folders in the specified folder
    folder_prefix = f"{folder}/"
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

            downloaded_file_name = download_data(bucket, file_key, asset_name)

            stac_item["assets"][asset_name] = {
                "href": downloaded_file_name,  # f"https://{workspace}.{workspaces_domain}/files/{bucket}/{file_subpath}",
                "type": mime_type,
            }
    # Mark the order as succeeded and upload the updated STAC item
    update_stac_order_status(stac_item, item_id, OrderStatus.SUCCEEDED.value)
    upload_stac_item(bucket, key, stac_item)
    send_pulsar_message(bucket, key)

    # Create local record of the order, to be used as the workflow output
    write_stac_item_and_catalog(stac_item, key.split("/")[-1], item_id)

    # Adding this for debugging purposes later on, just in case
    logging.info(f"Files in current working directory: {os.getcwd()}")
    logging.info(list(glob.iglob("./**/*", recursive=True)))


def update_stac_item_failure(bucket: str, key: str, item_id: str):
    """Update the STAC item with the failure order status"""
    stac_item = retrieve_stac_item(bucket, key)

    # Mark the order as failed and upload the updated STAC item
    update_stac_order_status(stac_item, item_id, OrderStatus.FAILED.value)
    upload_stac_item(bucket, key, stac_item)
    send_pulsar_message(bucket, key)

    # Create local record of attempted order, to be used as the workflow output
    write_stac_item_and_catalog(stac_item, key.split("/")[-1], item_id)


async def get_existing_order_details(item_id):
    planet_api_key = get_api_key_from_secret("api-keys", "planet-key")
    auth = planet.Auth.from_key(planet_api_key)

    session = planet.Session(auth=auth)
    orders_client = planet.OrdersClient(session=session)

    async for order in orders_client.list_orders():
        for product in order["products"]:
            for product_item_id in product["item_ids"]:
                if product_item_id == item_id:
                    return order

    return {}


def get_credentials() -> dict:
    """Get AWS credentials for delivery of Planet data"""

    return {
        "AccessKeyId": get_api_key_from_secret(
            "planet-aws-access-key-id", "planet-aws-access-key-id"
        ),
        "SecretAccessKey": get_api_key_from_secret(
            "planet-aws-secret-access-key", "planet-aws-secret-access-key"
        ),
    }


def main(
    stac_key: str, workspace_bucket: str, workspace_domain: str, product_bundle: str
):
    """Submit an order for an acquisition, retrieve the data, and update the STAC item"""
    # Workspace STAC item should already be generated and ingested, with an order status of ordered.
    logging.info(f"Retrieving STAC item {stac_key} from bucket {workspace_bucket}")

    try:
        # Submit an order for the given STAC item
        logging.info(f"Identified item as {stac_key} in {workspace_bucket}")
        stac_item = retrieve_stac_item(workspace_bucket, stac_key)

        item_id, collection_id = get_id_and_collection_from_stac(stac_item, stac_key)

        logging.info(f"Preparing to submit order for {item_id} in {collection_id}")

        order = asyncio.run(get_existing_order_details(item_id))
        logging.info(f"Existing order: {order}")

        order_status = order.get("state")
        logging.info(f"Order status: {order_status}")
        if order_status in ["queued", "running"]:
            order_id = order.get("id")
            logging.info(f"Order for {item_id} has already been submitted: {order_id}")
            update_stac_item_failure(workspace_bucket, stac_key, None)
            return

        if not order_status == "success":
            credentials = get_credentials()

            delivery_request = define_delivery(credentials, workspace_bucket)
            order_request = create_order_request(
                item_id, collection_id, delivery_request, product_bundle
            )

            asyncio.run(submit_order(order_request))

            order = asyncio.run(get_existing_order_details(item_id))

        order_id = order.get("id")
        logging.info(f"Found order ID {order_id}")

    except Exception as e:
        logging.error(f"Failed to submit order: {e}")
        update_stac_item_failure(workspace_bucket, stac_key, None)
        return

    staging_folder = f"planet/commercial-data/{order_id}"
    output_folder = f"planet/{item_id}"

    try:
        # Wait for data from planet to arrive, then move it to the workspace
        poll_s3_for_data(
            source_bucket=workspace_bucket, order_id=order_id, item_id=item_id
        )

        unzip_and_upload_to_s3(
            workspace_bucket,
            staging_folder,
            order_id,
            item_id,
        )
    except Exception as e:
        logging.error(f"Failed to retrieve data: {e}")
        update_stac_item_failure(workspace_bucket, stac_key, item_id)
        return
    update_stac_item_success(
        workspace_bucket, stac_key, output_folder, item_id, workspace_domain
    )


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
