import argparse
import asyncio
import glob
import json
import logging
import mimetypes
import os
from enum import Enum
from typing import List

import boto3
import pulsar
from planet_adaptor.api_utils import (
    create_order_request,
    define_delivery,
    get_aws_api_key_from_secret,
    get_planet_api_key,
    submit_order,
)
from planet_adaptor.s3_utils import (
    download_and_store_locally,
    poll_s3_for_data,
    retrieve_stac_item,
)
from planet_adaptor.stac_utils import (
    current_time_iso8601,
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


product_bundle_map = {
    "PSScene": {
        "Visual": {"name": "visual"},
        "General use": {"name": "analytic_8b_sr_udm2,analytic_sr_udm2"},
        "Analytic": {"name": "analytic_8b_sr_udm2,analytic_sr_udm2"},
        "Basic": {
            "name": "basic_analytic_8b_udm2,basic_analytic_udm2",
            "allow_clip": False,
        },
    },
    "SkySatCollect": {
        "Visual": {"name": "visual"},
        "General use": {"name": "pansharpened_udm2"},
        "Analytic": {"name": "analytic_sr_udm2"},
        "Basic": {"name": "analytic_udm2"},
    },
}


def update_stac_item_success(
    stac_item: dict,
    file_name: str,
    collection_id: str,
    order_name: str,
    directory: str,
    workspace: str,
    workspaces_bucket: str,
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
    update_stac_order_status(stac_item, order_name, OrderStatus.SUCCEEDED.value)

    # Update the 'updated' and 'published' fields to the current time
    current_time = current_time_iso8601()
    stac_item["properties"]["updated"] = current_time
    stac_item["properties"]["published"] = current_time

    # Create local record of the order, to be used as the workflow output
    write_stac_item_and_catalog(
        stac_item, file_name, collection_id, order_name, workspace, workspaces_bucket
    )

    # Adding this for debugging purposes later on, just in case
    logging.info(f"Files in current working directory: {os.getcwd()}")
    logging.info(list(glob.iglob("./**/*", recursive=True)))


def update_stac_item_failure(
    stac_item: dict,
    file_name: str,
    collection_id: str,
    reason: str,
    workspace: str,
    workspace_bucket: str,
    order_name: str,
) -> None:
    """Update the STAC item with the failure order status"""
    # Mark the order as failed in the local STAC item
    update_stac_order_status(stac_item, order_name, OrderStatus.FAILED.value)

    # Mark the reason for the failure in the local STAC item
    stac_item["properties"]["order_failure_reason"] = reason

    # Update the 'updated' field to the current time
    current_time = current_time_iso8601()
    stac_item["properties"]["updated"] = current_time

    # Create local record of attempted order, to be used as the workflow output
    write_stac_item_and_catalog(
        stac_item, file_name, collection_id, order_name, workspace, workspace_bucket
    )


def update_stac_item_ordered(
    stac_item: dict,
    collection_id: str,
    item_id: str,
    order_id: str,
    s3_bucket: str,
    pulsar_url: str,
    workspace: str,
):
    """Update the STAC item with the ordered order status"""
    logging.info(f"Updating STAC item with order ID: {order_id} to 'ordered' status.")
    # Mark the order as ordered in the local STAC item
    update_stac_order_status(stac_item, order_id, OrderStatus.ORDERED.value)

    # Update the 'updated' field to the current time
    current_time = current_time_iso8601()
    stac_item["properties"]["updated"] = current_time
    stac_item["properties"]["order:date"] = current_time

    # Ingest the updated STAC item to the catalog
    try:
        ingest_stac_item(
            stac_item, s3_bucket, pulsar_url, workspace, collection_id, item_id
        )
    except Exception as e:
        logging.error(f"Failed to ingest STAC item: {e}", exc_info=True)


def ingest_stac_item(
    stac_item: dict,
    s3_bucket: str,
    pulsar_url: str,
    workspace: str,
    collection_id: str,
    item_id: str,
):
    """Ingest the STAC item to the S3 bucket and send a Pulsar message"""
    # Upload the STAC item to S3
    s3_client = boto3.client("s3")
    parent_catalog_name = "commercial-data"

    item_key = (
        f"{workspace}/{parent_catalog_name}/planet/{collection_id}/{item_id}.json"
    )
    s3_client.put_object(Body=json.dumps(stac_item), Bucket=s3_bucket, Key=item_key)

    logging.info(
        f"Uploaded STAC item to S3 bucket '{s3_bucket}' with key '{item_key}'."
    )

    transformed_item_key = (
        f"transformed/catalogs/user/catalogs/{workspace}/catalogs/{parent_catalog_name}/catalogs/"
        f"planet/collections/{collection_id}/items/{item_id}.json"
    )
    s3_client.put_object(
        Body=json.dumps(stac_item), Bucket=s3_bucket, Key=transformed_item_key
    )

    logging.info(
        f"Uploaded STAC item to S3 bucket '{s3_bucket}' with key '{transformed_item_key}'."
    )

    # Send a Pulsar message
    pulsar_client = pulsar.Client(pulsar_url)
    producer = pulsar_client.create_producer(
        topic="transformed", producer_name=f"data_adaptor-{workspace}-{item_id}"
    )
    output_data = {
        "id": f"{workspace}/update_order",
        "workspace": workspace,
        "bucket_name": s3_bucket,
        "added_keys": [],
        "updated_keys": [transformed_item_key],
        "deleted_keys": [],
        "source": "/",
        "target": "/",
    }
    producer.send((json.dumps(output_data)).encode("utf-8"))
    logging.info(f"Sent Pulsar message {output_data}.")

    # Close the Pulsar client
    pulsar_client.close()


async def get_existing_order_details(workspace, order_name) -> dict:
    """Retrieve details of an existing order from the Planet API"""
    planet_api_key = get_planet_api_key(workspace)
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
        "AccessKeyId": get_aws_api_key_from_secret(
            "planet-aws-access-key-id", "planet-aws-access-key-id"
        ),
        "SecretAccessKey": get_aws_api_key_from_secret(
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
        self.order_status = get_key_from_stac(self.stac_json, "order:status")


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
    workspace: str,
    workspace_bucket: str,
    commercial_data_bucket: str,
    pulsar_url: str,
    product_bundle_category: str,
    coordinates: List,
    catalogue_dirs: List[str],
) -> None:
    """Submit an order for an acquisition, retrieve the data, and update the STAC item"""
    # Workspace STAC item should already be generated and ingested, with an order status of ordered.
    logging.info(
        f"Preparing {product_bundle_category} data for {workspace} for the following: {catalogue_dirs}"
    )
    stac_items: List[STACItem] = prepare_stac_items_to_order(catalogue_dirs)

    for stac_item in stac_items:
        collection_id = stac_item.collection_id
        if collection_id not in product_bundle_map.keys():
            raise NotImplementedError(
                f"Collection {collection_id} is not valid. Currently implemented collections are "
                f"{product_bundle_map.keys()}"
            )

        try:
            product_bundle = product_bundle_map[collection_id][product_bundle_category]

        except KeyError:
            raise NotImplementedError(
                f"Product bundle {product_bundle_category} is not valid. Currently implemented bundles are "
                f"{product_bundle_map[collection_id].keys()} for {collection_id}"
            )

        item_id = stac_item.item_id.rsplit("_", 1)[0]
        order_name = f"{stac_item.item_id}-{workspace}"

        logging.info(f"Coordinates: {coordinates}")
        if not verify_coordinates(coordinates):
            raise ValueError(f"Invalid coordinates: {coordinates}")

        delivery_folder = "planet/commercial-data/orders"

        try:
            # Submit an order for the given STAC item
            logging.info(f"Ordering stac item {item_id} in {collection_id}")

            order = asyncio.run(get_existing_order_details(workspace, order_name))
            logging.info(f"Existing order: {order}")

            order_status = order.get("state")
            logging.info(f"Order status: {order_status}")
            if order_status in ["queued", "running"]:
                submitted_order_id = order.get("id")
                reason = f"Order for {item_id} has already been submitted: {submitted_order_id}"
                logging.info(reason)
                update_stac_item_failure(
                    stac_item.stac_json,
                    stac_item.file_name,
                    stac_item.collection_id,
                    reason,
                    workspace,
                    workspace_bucket,
                    None,
                )
                return

            if not order_status == "success":
                credentials = get_credentials()

                delivery_request = define_delivery(
                    credentials, commercial_data_bucket, delivery_folder
                )
                order_request = create_order_request(
                    order_name,
                    item_id,
                    collection_id,
                    delivery_request,
                    product_bundle,
                    coordinates,
                )

                asyncio.run(submit_order(workspace, order_request))

                order = asyncio.run(get_existing_order_details(workspace, order_name))

            order_id = order.get("id")
            logging.info(f"Found order ID {order_id}")

        except Exception as e:
            reason = f"Failed to submit order: {e}"
            logging.error(reason, exc_info=True)
            update_stac_item_failure(
                stac_item.stac_json,
                stac_item.file_name,
                stac_item.collection_id,
                reason,
                workspace,
                workspace_bucket,
                order_name,
            )
            return

        # Update the STAC record after submitting the order
        update_stac_item_ordered(
            stac_item.stac_json,
            stac_item.collection_id,
            stac_item.item_id,
            order_id,
            workspace_bucket,
            pulsar_url,
            workspace,
        )

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
            reason = f"Failed to retrieve data: {e}"
            logging.error(reason, exc_info=True)
            update_stac_item_failure(
                stac_item.stac_json,
                stac_item.file_name,
                stac_item.collection_id,
                reason,
                workspace,
                workspace_bucket,
                order_id,
            )
            return
        update_stac_item_success(
            stac_item.stac_json,
            stac_item.file_name,
            stac_item.collection_id,
            order_name,
            "assets",
            workspace,
            workspace_bucket,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Order Planet data")
    parser.add_argument("workspace", type=str, help="Workspace name")
    parser.add_argument("workspace_bucket", type=str, help="Workspace bucket")
    parser.add_argument(
        "commercial_data_bucket", type=str, help="Commercial data bucket"
    )
    parser.add_argument("pulsar_url", type=str, help="Pulsar URL")
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
        args.workspace_bucket,
        args.commercial_data_bucket,
        args.pulsar_url,
        args.product_bundle,
        coordinates,
        args.catalogue_dirs,
    )
