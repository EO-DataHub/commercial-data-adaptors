import json
import logging
from typing import Tuple


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


def get_id_and_collection_from_stac(stac_item: dict, key: str) -> Tuple[str, str]:
    """Extract the acquisition ID from a STAC item"""
    collection_id = stac_item.get("properties", {}).get("item_type")
    item_id = stac_item.get("id")
    if not item_id:
        raise ValueError(f"Item ID not found in STAC item '{key}'.")
    if not collection_id:
        raise ValueError(f"Collection not found in STAC item '{key}'.")
    return item_id, collection_id
