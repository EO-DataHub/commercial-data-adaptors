import asyncio
import glob
import json
import logging
import mimetypes
import os
import sys
from enum import Enum
from typing import List

from planet_adaptor.api_utils import (
    create_order_request,
    define_delivery,
    get_api_key_from_secret,
    submit_order,
)
from planet_adaptor.s3_utils import (
    download_and_store_locally,
    poll_s3_for_data,
    retrieve_stac_item,
)
from planet_adaptor.stac_utils import (
    get_item_hrefs_from_catalogue,
    get_key_from_stac,
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
    stac_item: dict, file_name: str, order_id: str, directory: str
):
    """Update the STAC item with the assets and success order status"""
    # Add all files in the directory as assets to the STAC item
    for root, _, files in os.walk(directory):
        for asset in files:
            asset_path = os.path.join(root, asset)
            asset_name = os.path.basename(asset_path)

            # Determine the MIME type of the file
            mime_type, _ = mimetypes.guess_type(asset_path)
            if mime_type is None:
                mime_type = "application/octet-stream"  # Default MIME type

            # Add asset link to the file
            stac_item["assets"][asset_name] = {
                "href": asset_path,
                "type": mime_type,
            }
    # Mark the order as succeeded and upload the updated STAC item
    update_stac_order_status(stac_item, order_id, OrderStatus.SUCCEEDED.value)

    # Create local record of the order, to be used as the workflow output
    write_stac_item_and_catalog(stac_item, file_name, order_id)

    # Adding this for debugging purposes later on, just in case
    logging.info(f"Files in current working directory: {os.getcwd()}")
    logging.info(list(glob.iglob("./**/*", recursive=True)))


def update_stac_item_failure(stac_item: dict, file_name: str, order_id: str) -> None:
    """Update the STAC item with the failure order status"""
    # Mark the order as failed in the local STAC item
    update_stac_order_status(stac_item, order_id, OrderStatus.FAILED.value)

    # Create local record of attempted order, to be used as the workflow output
    write_stac_item_and_catalog(stac_item, file_name, order_id)


async def get_existing_order_details(item_id) -> dict:
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


class STACItem:
    """Class to represent a STAC item and its properties"""

    def __init__(self, stac_item_path: str):
        self.file_path = stac_item_path
        self.file_name = os.path.basename(stac_item_path)
        self.stac_json = retrieve_stac_item(stac_item_path)
        self.item_id = get_key_from_stac(self.stac_json, "id")
        self.collection_id = get_key_from_stac(self.stac_json, "properties.item_type")
        self.coordinates = get_key_from_stac(self.stac_json, "geometry.coordinates")
        self.order_status = get_key_from_stac(self.stac_json, "order.status")


def prepare_stac_items_to_order(catalogue_dirs: List[str]) -> List[STACItem]:
    """Prepare a list of STAC items to order"""
    stac_item_paths = []
    for catalogue_dir in catalogue_dirs:
        if not os.path.exists(catalogue_dir):
            raise FileNotFoundError(f"Catalogue directory {catalogue_dir} not found.")
        stac_item_paths += get_item_hrefs_from_catalogue(catalogue_dir)
    if not stac_item_paths:
        raise ValueError("No STAC items found in the given directories.")
    logging.info(f"STAC item paths: {stac_item_paths}")

    stac_items = []

    for stac_item_path in stac_item_paths:
        stac_item_to_add = STACItem(stac_item_path)
        stac_items.append(stac_item_to_add)

    return stac_items


def main(
    commercial_data_bucket: str, product_bundle: str, catalogue_dirs: List[str]
) -> None:
    """Submit an order for an acquisition, retrieve the data, and update the STAC item"""
    # Workspace STAC item should already be generated and ingested, with an order status of ordered.
    logging.info(catalogue_dirs)
    stac_items: List[STACItem] = prepare_stac_items_to_order(catalogue_dirs)

    for stac_item in stac_items:
        try:
            # Submit an order for the given STAC item
            logging.info(
                f"Ordering stac item {stac_item.item_id} in {stac_item.collection_id}"
            )

            order = asyncio.run(get_existing_order_details(stac_item.item_id))
            logging.info(f"Existing order: {order}")

            order_status = order.get("state")
            logging.info(f"Order status: {order_status}")
            if order_status in ["queued", "running"]:
                order_id = order.get("id")
                logging.info(
                    f"Order for {stac_item.item_id} has already been submitted: {order_id}"
                )
                update_stac_item_failure(stac_item.stac_json, stac_item.file_name, None)
                return

            if not order_status == "success":
                credentials = get_credentials()

                delivery_request = define_delivery(credentials, commercial_data_bucket)
                order_request = create_order_request(
                    stac_item.item_id,
                    stac_item.collection_id,
                    delivery_request,
                    product_bundle,
                )

                asyncio.run(submit_order(order_request))

                order = asyncio.run(get_existing_order_details(stac_item.item_id))

            order_id = order.get("id")
            logging.info(f"Found order ID {order_id}")

        except Exception as e:
            logging.error(f"Failed to submit order: {e}")
            update_stac_item_failure(stac_item.stac_json, stac_item.file_name, None)
            return

        staging_folder = f"planet/commercial-data/{order_id}"

        try:
            # Wait for data from planet to arrive, then move it to the workspace
            poll_s3_for_data(source_bucket=commercial_data_bucket, order_id=order_id)

            download_and_store_locally(commercial_data_bucket, staging_folder, "assets")
        except Exception as e:
            logging.error(f"Failed to retrieve data: {e}")
            update_stac_item_failure(stac_item.stac_json, stac_item.file_name, order_id)
            return
        update_stac_item_success(
            stac_item.stac_json, stac_item.file_name, order_id, "assets"
        )


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3:])
