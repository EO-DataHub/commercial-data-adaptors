import json
import logging
import mimetypes
import os
from enum import Enum


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


def write_stac_item_and_catalog(stac_item: dict, stac_item_filename: str, item_id: str):
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
        "description": f"Root catalog for order {stac_item_filename}-{item_id}",
        "links": [
            {"rel": "self", "href": "catalog.json", "type": "application/json"},
            {"rel": "item", "href": stac_item_filename, "type": "application/json"},
        ],
    }

    # Write the STAC catalog to a file
    with open("catalog.json", "w") as f:
        json.dump(stac_catalog, f, indent=2)
    logging.info("Created STAC catalog catalog.json locally.")
    logging.debug(f"STAC catalog: {stac_catalog}")


def update_stac_order_status(stac_item: dict, item_id: str, order_status: str):
    """Update the STAC item with the order status using the STAC Order extension"""
    # Update or add fields relating to the order
    if "properties" not in stac_item:
        stac_item["properties"] = {}

    if item_id is not None:
        stac_item["properties"]["order.id"] = item_id
    stac_item["properties"]["order.status"] = order_status

    # Update or add the STAC extension if not already present
    order_extension_url = "https://stac-extensions.github.io/order/v1.1.0/schema.json"
    if "stac_extensions" not in stac_item:
        stac_item["stac_extensions"] = []

    if order_extension_url not in stac_item["stac_extensions"]:
        stac_item["stac_extensions"].append(order_extension_url)


def get_acquisition_id_from_stac(stac_item: dict, key: str) -> str:
    """Extract the acquisition ID from a STAC item"""
    acquisition_id = stac_item.get("properties", {}).get("acquisition_id")
    if not acquisition_id:
        raise ValueError(f"Acquisition ID not found in STAC item '{key}'.")
    return acquisition_id


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


def update_stac_item_failure(
    stac_item: dict, file_name: str, order_id: str = None
) -> None:
    """Update the STAC item with the failure order status"""
    # Mark the order as failed in the local STAC item
    update_stac_order_status(stac_item, order_id, OrderStatus.FAILED.value)

    # Create local record of attempted order, to be used as the workflow output
    write_stac_item_and_catalog(stac_item, file_name, order_id)


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
