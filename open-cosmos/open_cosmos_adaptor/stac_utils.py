import json
import logging
import os
from datetime import UTC, datetime
from enum import Enum

import boto3
import pulsar
from pystac import Item, Link

Coordinate = list[float] | tuple[float, float]


# As defined in the STAC order extension. See https://github.com/stac-extensions/order#orderstatus
class OrderStatus(Enum):
    ORDERABLE = "orderable"
    ORDERED = "ordered"
    PENDING = "pending"
    SHIPPING = "shipping"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELED = "canceled"


def write_stac_item_and_catalog(
    stac_item: Item,
    stac_item_filename: str,
    collection_id: str,
    workspace: str,
    workspaces_bucket: str,
) -> None:
    """Creates local catalog containing final STAC item to be used as a record for the order"""

    # Rewrite STAC links to point to local files only
    stac_item.links = [
        Link(rel="self", target=stac_item_filename, media_type="application/json"),
        Link(rel="parent", target="catalog.json", media_type="application/json"),
    ]

    # Write the STAC item to a file.
    with open(stac_item_filename, "w") as f:
        json.dump(stac_item.to_dict(), f, indent=2)
    logging.info(f"Created STAC item '{stac_item_filename}' locally.")
    logging.info(f"STAC item: {stac_item}")

    # Create containing STAC catalog
    try:
        # obtain the existing catalog from s3 if possible
        s3_client = boto3.client("s3")
        key = f"{workspace}/commercial-data/open-cosmos.json"
        logging.info(f"Retrieving existing catalog from s3: {key}, {workspaces_bucket}")
        response = s3_client.get_object(Bucket=workspaces_bucket, Key=key)
        stac_catalog = json.loads(response["Body"].read())

    except Exception as e:
        logging.info(f"Failed to retrieve existing collection from s3: {e}")
        logging.info("Creating default collection")
        stac_catalog = {
            "stac_version": "1.0.0",
            "id": "open-cosmos",
            "type": "Catalog",
            "description": "Order records for Open Cosmos, including completed purchases with their associated assets, as well as records of ongoing and failed orders.",
            "links": [],
        }
    stac_catalog["links"] = [
        {"rel": "self", "href": "catalog.json", "type": "application/json"},
        {"rel": "child", "href": "collection.json", "type": "application/json"},
    ]

    with open("catalog.json", "w") as f:
        json.dump(stac_catalog, f, indent=2)

    logging.info("Created STAC catalog catalog.json locally.")
    logging.info(f"STAC catalog: {stac_catalog}")

    # Create the STAC collection.
    try:
        # obtain the existing collection from s3 if possible
        s3_client = boto3.client("s3")
        key = f"{workspace}/commercial-data/open-cosmos/{collection_id}.json"
        logging.info(f"Retrieving existing collection from s3: {key}, {workspaces_bucket}")
        response = s3_client.get_object(Bucket=workspaces_bucket, Key=key)
        stac_collection = json.loads(response["Body"].read())

    except Exception as e:
        logging.info(f"Failed to retrieve existing collection from s3: {e}")
        logging.info("Creating default collection")
        stac_collection = {
            "stac_version": "1.0.0",
            "id": collection_id,
            "type": "Collection",
            "description": f"Order records for {collection_id.capitalize().replace('_', ' ')}, including completed purchases with their associated assets, as well as records of ongoing and failed orders.",
            "license": "proprietary",
            "links": [],
            "keywords": ["open-cosmos"],
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {
                    "interval": [
                        [
                            "2010-01-01T00:00:00Z",
                            datetime.now(UTC).isoformat(),
                        ]
                    ]
                },
            },
        }
    stac_collection["links"] = [
        {"rel": "self", "href": "collection.json", "type": "application/json"},
        {"rel": "root", "href": "catalog.json", "type": "application/json"},
        {"rel": "parent", "href": "catalog.json", "type": "application/json"},
        {"rel": "item", "href": stac_item_filename, "type": "application/json"},
    ]

    # Write the STAC catalog to a file
    with open("collection.json", "w") as f:
        json.dump(stac_collection, f, indent=2)
    logging.info("Created STAC collection collection.json locally.")
    logging.debug(f"STAC collection: {stac_collection}")
    logging.info(f"STAC collection: {stac_collection}")


def update_stac_order_status(stac_item: Item, order_id: str | None, order_status: str) -> None:
    """Update the STAC item with the order status using the STAC Order extension"""

    if order_id is not None:
        stac_item.properties["order:id"] = order_id

    stac_item.properties["order:status"] = order_status

    # Update or add the STAC extension if not already present
    order_extension_url = "https://stac-extensions.github.io/order/v1.1.0/schema.json"

    if order_extension_url not in stac_item.stac_extensions:
        stac_item.stac_extensions.append(order_extension_url)


def get_item_hrefs_from_catalogue(catalogue_dir: str) -> list:
    """Return a list of all hrefs to items in the STAC catalog"""
    catalog_path = os.path.join(catalogue_dir, "catalog.json")
    if not os.path.exists(catalog_path):
        raise FileNotFoundError(f"The file {catalog_path} does not exist.")

    with open(catalog_path, encoding="utf-8") as f:
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
    if not isinstance(latitude, (int, float)) or not isinstance(longitude, (int, float)):
        logging.warning(f"Invalid coordinate type. {longitude}: {type(longitude)}, {latitude}: {type(latitude)}")
        return False
    if not (-90 <= latitude <= 90) or not (-180 <= longitude <= 180):
        logging.warning(f"Invalid coordinate value: longitude={longitude}, latitude={latitude}")
        return False
    return True


def verify_coordinates(coordinates: list[list[Coordinate]]) -> bool:
    """Verify that a list of coordinates is valid."""
    return all(all(is_valid_coordinate(coord) for coord in polygons) for polygons in coordinates)


def update_stac_item_success(
    stac_item: Item,
    file_name: str,
    collection_id: str,
    order_name: str,
    directory: str,
    workspace: str,
    workspaces_bucket: str,
) -> None:
    """Update the STAC item with the assets and success order status"""
    # Add all files in the directory as assets to the STAC item
    for asset in stac_item.assets.values():
        filename = os.path.basename(asset.href)
        asset.href = f"{directory}/{filename}"

    # Mark the order as succeeded and upload the updated STAC item
    update_stac_order_status(stac_item, order_name, OrderStatus.SUCCEEDED.value)

    # Update the 'updated' and 'published' fields to the current time
    current_time = datetime.now(UTC).isoformat()
    stac_item.properties["updated"] = current_time
    stac_item.properties["published"] = current_time

    # Create local record of the order, to be used as the workflow output
    write_stac_item_and_catalog(stac_item, file_name, collection_id, workspace, workspaces_bucket)


def update_stac_item_failure(
    stac_item: Item,
    file_name: str,
    collection_id: str,
    reason: str,
    workspace: str,
    workspace_bucket: str,
    order_id: str | None = None,
) -> None:
    """Update the STAC item with the failure order status"""
    # Mark the order as failed in the local STAC item
    update_stac_order_status(stac_item, order_id, OrderStatus.FAILED.value)

    # Mark the reason for the failure in the local STAC item
    stac_item.properties["order_failure_reason"] = reason

    # Update the 'updated' field to the current time
    stac_item.properties["updated"] = datetime.now(UTC).isoformat()

    # Create local record of attempted order, to be used as the workflow output
    write_stac_item_and_catalog(stac_item, file_name, collection_id, workspace, workspace_bucket)


def update_stac_item_ordered(
    stac_item: Item,
    collection_id: str,
    file_name: str,
    order_id: str,
    s3_bucket: str,
    pulsar_url: str,
    workspace: str,
) -> None:
    """Update the STAC item with the ordered order status"""
    logging.info(f"Updating STAC item with order ID: {order_id} to 'ordered' status.")
    # Mark the order as ordered in the local STAC item
    update_stac_order_status(stac_item, order_id, OrderStatus.ORDERED.value)

    # Update the 'updated' field to the current time
    current_time = datetime.now(UTC).isoformat()
    stac_item.properties["updated"] = current_time
    stac_item.properties["order:date"] = current_time

    # Ingest the updated STAC item to the catalog
    try:
        ingest_stac_item(stac_item, s3_bucket, pulsar_url, workspace, collection_id, file_name)
    except Exception as e:
        logging.error(f"Failed to ingest STAC item: {e}", exc_info=True)


def ingest_stac_item(
    stac_item: Item,
    s3_bucket: str,
    pulsar_url: str,
    workspace: str,
    collection_id: str,
    file_name: str,
) -> None:
    """Copy the STAC item to the workspace's S3 bucket and send a Pulsar message to begin the purchase workflow."""
    s3_client = boto3.client("s3")
    parent_catalog_name = "commercial-data"

    item_key = f"{workspace}/{parent_catalog_name}/open-cosmos/{collection_id}/{file_name}"
    s3_client.put_object(Body=json.dumps(stac_item.to_dict()), Bucket=s3_bucket, Key=item_key)

    logging.info(f"Uploaded STAC item to S3 bucket '{s3_bucket}' with key '{item_key}'.")

    transformed_item_key = (
        f"transformed/catalogs/user/catalogs/{workspace}/catalogs/{parent_catalog_name}/catalogs/"
        f"open-cosmos/collections/{collection_id}/items/{file_name}"
    )
    s3_client.put_object(Body=json.dumps(stac_item.to_dict()), Bucket=s3_bucket, Key=transformed_item_key)

    logging.info(f"Uploaded STAC item to S3 bucket '{s3_bucket}' with key '{transformed_item_key}'.")

    pulsar_client = pulsar.Client(pulsar_url)
    producer = pulsar_client.create_producer(
        topic="transformed", producer_name=f"data_adaptor-{workspace}-{file_name}"
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

    pulsar_client.close()
