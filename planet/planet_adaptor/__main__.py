import argparse
import asyncio
import glob
import hashlib
import json
import logging
import mimetypes
import os
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
    verify_coordinates,
    write_stac_item_and_catalog,
)

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


def update_stac_item_failure(stac_item: dict, file_name: str, item_id: str) -> None:
    """Update the STAC item with the failure order status"""
    # Mark the order as failed in the local STAC item
    update_stac_order_status(stac_item, item_id, OrderStatus.FAILED.value)

    # Create local record of attempted order, to be used as the workflow output
    write_stac_item_and_catalog(stac_item, file_name, item_id)


async def get_existing_order_details(order_name) -> dict:
    planet_api_key = get_api_key_from_secret("api-keys", "planet-key")
    auth = planet.Auth.from_key(planet_api_key)

    session = planet.Session(auth=auth)
    orders_client = planet.OrdersClient(session=session)

    async for order in orders_client.list_orders():
        if order["name"] == order_name:
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


def hash_aoi(coordinates):
    """Converts coordinates to a hash value for a unique item identifier for each AOI"""
    return str(hashlib.md5(str(coordinates).encode("utf-8")).hexdigest())


def main(
    workspace: str,
    commercial_data_bucket: str,
    product_bundle: str,
    coordinates: List,
    catalogue_dirs: List[str],
) -> None:
    """Submit an order for an acquisition, retrieve the data, and update the STAC item"""
    # Workspace STAC item should already be generated and ingested, with an order status of ordered.
    logging.info(catalogue_dirs)
    stac_items: List[STACItem] = prepare_stac_items_to_order(catalogue_dirs)

    for stac_item in stac_items:
        if coordinates:
            order_name = f"{stac_item.item_id}-{workspace}-{hash_aoi(coordinates)}"
        else:
            coordinates = stac_item.coordinates
            order_name = f"{stac_item.item_id}-{workspace}"

        logging.info(f"Coordinates: {coordinates}")
        if not verify_coordinates(coordinates):
            raise ValueError(f"Invalid coordinates: {coordinates}")

        delivery_folder = "planet/commercial-data/orders"

        try:
            # Submit an order for the given STAC item
            logging.info(
                f"Ordering stac item {stac_item.item_id} in {stac_item.collection_id}"
            )

            order = asyncio.run(get_existing_order_details(order_name))
            logging.info(f"Existing order: {order}")

            order_status = order.get("state")
            logging.info(f"Order status: {order_status}")
            if order_status in ["queued", "running"]:
                submitted_order_id = order.get("id")
                logging.info(
                    f"Order for {stac_item.item_id} has already been submitted: {submitted_order_id}"
                )
                update_stac_item_failure(stac_item.stac_json, stac_item.file_name, None)
                return

            if not order_status == "success":
                credentials = get_credentials()

                delivery_request = define_delivery(
                    credentials, commercial_data_bucket, delivery_folder
                )
                order_request = create_order_request(
                    order_name,
                    stac_item.item_id,
                    stac_item.collection_id,
                    delivery_request,
                    product_bundle,
                    coordinates,
                )

                asyncio.run(submit_order(order_request))

                order = asyncio.run(get_existing_order_details(order_name))

            order_id = order.get("id")
            logging.info(f"Found order ID {order_id}")

        except Exception as e:
            logging.error(f"Failed to submit order: {e}", exc_info=True)
            update_stac_item_failure(stac_item.stac_json, stac_item.file_name, None)
            return

        try:
            # Wait for data from planet to arrive, then move it to the workspace
            poll_s3_for_data(
                source_bucket=commercial_data_bucket,
                order_id=order_id,
                folder=delivery_folder,
            )

            download_and_store_locally(
                commercial_data_bucket, f"{delivery_folder}/{order_id}", "assets"
            )
        except Exception as e:
            logging.error(f"Failed to retrieve data: {e}", exc_info=True)
            update_stac_item_failure(
                stac_item.stac_json, stac_item.file_name, stac_item.item_id
            )
            return
        update_stac_item_success(
            stac_item.stac_json, stac_item.file_name, order_name, "assets"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Order Planet data")
    parser.add_argument("workspace", type=str, help="Workspace name")
    parser.add_argument(
        "commercial_data_bucket", type=str, help="Commercial data bucket"
    )
    parser.add_argument("product_bundle", type=str, help="Product bundle")
    parser.add_argument("coordinates", type=str, help="Stringified list of coordinates")
    parser.add_argument(
        "catalogue_dirs",
        nargs="+",
        help="List of catalogue directories",
    )

    args = parser.parse_args()

    coordinates = json.loads(args.coordinates)

    main(
        args.workspace,
        args.commercial_data_bucket,
        args.product_bundle,
        coordinates,
        args.catalogue_dirs,
    )
