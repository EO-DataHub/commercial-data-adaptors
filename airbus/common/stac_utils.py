import json
import logging
import mimetypes
import os
from datetime import datetime, timezone
from enum import Enum
from typing import List, Union

import boto3
import pulsar

Coordinate = Union[List[float], tuple[float, float]]


class OrderStatus(Enum):
    ORDERABLE = "orderable"
    ORDERED = "ordered"
    PENDING = "pending"
    SHIPPING = "shipping"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELED = "canceled"


def retrieve_stac_item(file_path: str) -> dict:
    """Retrieve a STAC item from a local JSON file"""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file {file_path} does not exist.")

    with open(file_path, "r", encoding="utf-8") as f:
        stac_item = json.load(f)
    return stac_item


def write_stac_item_and_catalog(
    stac_item: dict, stac_item_filename: str, collection_id: str, item_id: str
):
    """Creates local catalog containing final STAC item to be used as a record for the order"""
    # Rewrite STAC links to point to local files only
    stac_item["links"] = [
        {"rel": "self", "href": stac_item_filename, "type": "application/json"},
        {"rel": "parent", "href": "catalog.json", "type": "application/json"},
    ]

    # Write the STAC item to a file
    with open(stac_item_filename, "w") as f:
        json.dump(stac_item, f, indent=2)
    logging.info(f"Created STAC item '{stac_item_filename}' locally.")
    logging.debug(f"STAC item: {stac_item}")

    # If not item_id, the order has failed
    if not item_id:
        item_id = "Failed"

    # Create containing STAC catalog
    stac_catalog = {
        "stac_version": "1.0.0",
        "id": "catalog",
        "type": "Catalog",
        "description": "Purchased Airbus satellite imagery, including both completed purchases and ongoing order records",
        "links": [
            {"rel": "self", "href": "catalog.json", "type": "application/json"},
            {"rel": "child", "href": "collection.json", "type": "application/json"},
        ],
    }

    # Write the STAC catalog to a file
    with open("catalog.json", "w") as f:
        json.dump(stac_catalog, f, indent=2)
    logging.info("Created STAC catalog catalog.json locally.")
    logging.debug(f"STAC catalog: {stac_catalog}")

    stac_collection = {
        "id": collection_id,
        "type": "Collection",
        "description": f"Purchased {collection_id.capitalize().replace('_', ' ')} satellite imagery, including both completed purchases and ongoing order records",
        "links": [
            {"rel": "self", "href": "collection.json", "type": "application/json"},
            {"rel": "root", "href": "catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "catalog.json", "type": "application/json"},
            {"rel": "item", "href": stac_item_filename, "type": "application/json"},
        ],
    }

    # Write the STAC catalog to a file
    with open("collection.json", "w") as f:
        json.dump(stac_collection, f, indent=2)
    logging.info("Created STAC collection collection.json locally.")
    logging.debug(f"STAC collection: {stac_collection}")


def update_stac_order_status(stac_item: dict, order_id: str, order_status: str):
    """Update the STAC item with the order status using the STAC Order extension"""
    # Update or add fields relating to the order
    if "properties" not in stac_item:
        stac_item["properties"] = {}

    if order_id is not None:
        stac_item["properties"]["order.id"] = order_id
    stac_item["properties"]["order.status"] = order_status

    # Update or add the STAC extension if not already present
    order_extension_url = "https://stac-extensions.github.io/order/v1.1.0/schema.json"
    if "stac_extensions" not in stac_item:
        stac_item["stac_extensions"] = []

    if order_extension_url not in stac_item["stac_extensions"]:
        stac_item["stac_extensions"].append(order_extension_url)


def get_key_from_stac(stac_item: dict, key: str):
    """Extract a nested key from a STAC item. Key given as a dot-separated string."""
    parts = key.split(".")
    value = stac_item
    for part in parts:
        value = value.get(part)
        if value is None:
            logging.info(f"{part} not found in STAC item.")
            return None
    logging.info(f"Retrieved {key} from STAC item: {value}")
    return value


def ingest_stac_item(
    stac_item: dict, s3_bucket, pulsar_url, workspace, collection_id, item_id
):
    # Upload the STAC item to S3
    s3_client = boto3.client("s3")
    parent_catalog_name = "commercial-data"

    item_key = (
        f"{workspace}/{parent_catalog_name}/airbus/{collection_id}/{item_id}.json"
    )
    s3_client.put_object(Body=json.dumps(stac_item), Bucket=s3_bucket, Key=item_key)

    logging.info(
        f"Uploaded STAC item to S3 bucket '{s3_bucket}' with key '{item_key}'."
    )

    # Send a Pulsar message
    pulsar_client = pulsar.Client(pulsar_url)
    producer = pulsar_client.create_producer(
        topic="harvested", producer_name=f"data_adaptor-{workspace}-{item_id}"
    )
    output_data = {
        "id": f"{workspace}/update_order",
        "workspace": workspace,
        "bucket_name": s3_bucket,
        "added_keys": [],
        "updated_keys": [item_key],
        "deleted_keys": [],
        "source": workspace,
        "target": f"user-datasets/{workspace}",
    }
    producer.send((json.dumps(output_data)).encode("utf-8"))
    logging.info(f"Sent Pulsar message {output_data}.")

    # Close the Pulsar client
    pulsar_client.close()


def update_stac_item_failure(
    stac_item: dict,
    file_name: str,
    collection_id: str,
    reason: str,
    order_id: str = None,
) -> None:
    """Update the STAC item with the failure order status"""
    # Mark the order as failed in the local STAC item
    update_stac_order_status(stac_item, order_id, OrderStatus.FAILED.value)

    # Mark the reason for the failure in the local STAC item
    stac_item["properties"]["order_failure_reason"] = reason

    # Update the 'updated' field to the current time
    current_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    stac_item["properties"]["updated"] = current_time

    # Create local record of attempted order, to be used as the workflow output
    write_stac_item_and_catalog(stac_item, file_name, collection_id, order_id)


def update_stac_item_ordered(
    stac_item: dict,
    collection_id,
    item_id,
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
    current_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    stac_item["properties"]["updated"] = current_time
    stac_item["properties"]["order.date"] = current_time

    # Ingest the updated STAC item to the catalog
    try:
        ingest_stac_item(
            stac_item, s3_bucket, pulsar_url, workspace, collection_id, item_id
        )
    except Exception as e:
        logging.error(f"Failed to ingest STAC item: {e}", exc_info=True)


def update_stac_item_success(
    stac_item: dict, file_name: str, collection_id, order_id: str, directory: str
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

    # Update the 'updated' and 'published' fields to the current time
    current_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    stac_item["properties"]["updated"] = current_time
    stac_item["properties"]["published"] = current_time

    # Create local record of the order, to be used as the workflow output
    write_stac_item_and_catalog(stac_item, file_name, collection_id, order_id)


def get_item_hrefs_from_catalogue(catalogue_dir: str) -> list:
    """Return a list of all hrefs to items in the STAC catalog"""
    catalog_path = os.path.join(catalogue_dir, "catalog.json")
    if not os.path.exists(catalog_path):
        raise FileNotFoundError(f"The file {catalog_path} does not exist.")

    with open(catalog_path, "r", encoding="utf-8") as f:
        catalog = json.load(f)

    item_hrefs = []
    for link in catalog.get("links", []):
        if link.get("rel") == "item":
            href = link.get("href")
            absolute_href = os.path.normpath(os.path.join(catalogue_dir, href))
            item_hrefs.append(absolute_href)

    return item_hrefs


def is_valid_coordinate(coordinate: Coordinate) -> bool:
    """Check if a single coordinate is valid."""
    if not isinstance(coordinate, (list, tuple)) or len(coordinate) != 2:
        logging.warning(
            f"Invalid coordinate format: {coordinate}, type: {type(coordinate)}, length: {len(coordinate)}"
        )
        return False
    longitude, latitude = coordinate
    if not isinstance(latitude, (int, float)) or not isinstance(
        longitude, (int, float)
    ):
        logging.warning(
            f"Invalid coordinate type. {longitude}: {type(longitude)}, {latitude}: {type(latitude)}"
        )
        return False
    if not (-90 <= latitude <= 90) or not (-180 <= longitude <= 180):
        logging.warning(
            f"Invalid coordinate value: longitude={longitude}, latitude={latitude}"
        )
        return False
    return True


def verify_coordinates(coordinates: List[List[Coordinate]]) -> bool:
    """Verify that a list of coordinates is valid."""
    return all(
        all(is_valid_coordinate(coord) for coord in polygons)
        for polygons in coordinates
    )
