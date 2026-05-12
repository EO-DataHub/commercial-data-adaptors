import argparse
import logging
import os
from pathlib import Path

import requests

from open_cosmos_adaptor.auth_utils import get_access_token, get_contract_info
from s3_utils import download_and_store_locally
from stac_utils import get_item_hrefs_from_catalogue, update_stac_item_failure, \
    update_stac_item_success, update_stac_item_ordered

from pystac import Item

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def prepare_stac_items_to_order(catalogue_dirs: list[str]) -> dict[str, Item]:
    """Prepare a list of STAC items to order"""
    stac_item_paths = []
    for catalogue_dir in catalogue_dirs:
        if not os.path.exists(catalogue_dir):
            raise FileNotFoundError(f"Catalogue directory {catalogue_dir} not found.")
        stac_item_paths += get_item_hrefs_from_catalogue(catalogue_dir)
    if not stac_item_paths:
        raise ValueError("No STAC items found in the given directories.")
    logging.info(f"STAC item paths: {stac_item_paths}")

    new_items = {}

    for stac_item_path in stac_item_paths:
        item = Item.from_file(stac_item_path)
        new_items[os.path.basename(stac_item_path)] = item

    return new_items


def create_order_request(collection_id: str, item_id: str, level: str, organisation_id: int, contract_id: int) -> dict:
    url = "https://app.open-cosmos.com/api/data/v0/order/orders"

    order = {
        "type": "IMAGE",
        "data": {
            "order_line_items": [
                {
                    "collection": collection_id,
                    "item": item_id,
                    "level": level
                }
            ]
        },
        "organisation": organisation_id,
        "contract_id": contract_id
    }

    headers = {
        "Authorization": f"Bearer {get_access_token()}"
    }

    r = requests.post(url, json=order, headers=headers)
    r.raise_for_status()

    return r.json()


def main(
    workspace: str,
    workspace_bucket: str,
    commercial_data_bucket: str,
    pulsar_url: str,
    catalogue_dirs: list[str],
) -> None:
    logging.info(f"Preparing Open Cosmos data for {workspace} for the following: {catalogue_dirs}")
    new_stac_items: dict[str, Item] = prepare_stac_items_to_order(catalogue_dirs)
    contract_info = get_contract_info()

    for file_name, stac_item in new_stac_items.items():
        collection_id = stac_item.collection_id
        if collection_id is None:
            raise ValueError(f"Collection ID is None for item {stac_item.id}")

        order_name = f"{stac_item.id}-{workspace}"

        delivery_folder = Path("opencosmos/commercial-data/orders")

        # Submit an order for the given STAC item
        logging.info(f"Ordering stac item {stac_item.id} in {collection_id}")

        try:
            order = create_order_request(
                collection_id,
                stac_item.id,
                stac_item.properties["processing:level"],
                contract_info.organisation_id,
                contract_info.contract_id
            )

            order_id = order["data"].get("id")
            if order_id is None:
                raise ValueError(f"No order ID found for order {order_name}")

            if order["data"]["status"] != "PAID":
                raise ValueError(f"Order {order_name} is not paid")

            logging.info(f"Found order ID {order_id}")

        except Exception as e:
            reason = f"Failed to submit order: {e}"
            logging.error(reason, exc_info=True)
            update_stac_item_failure(
                stac_item,
                file_name,
                stac_item.collection_id,
                reason,
                workspace,
                workspace_bucket,
                order_name,
            )
            return

        # Update the STAC record after submitting the order
        update_stac_item_ordered(
            stac_item,
            stac_item.collection_id,
            stac_item.id,
            order_id,
            workspace_bucket,
            pulsar_url,
            workspace,
        )

        try:
            download_and_store_locally(stac_item, delivery_folder / order_id, Path(order_id))
        except Exception as e:
            reason = f"Failed to retrieve data: {e}"
            logging.error(reason, exc_info=True)
            update_stac_item_failure(
                stac_item,
                file_name,
                stac_item.collection_id,
                reason,
                workspace,
                workspace_bucket,
                order_id,
            )
            return
        update_stac_item_success(
            stac_item,
            file_name,
            stac_item.collection_id,
            order_id,
            order_id,
            workspace,
            workspace_bucket,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Order Open Cosmos data")
    parser.add_argument("workspace", type=str, help="Workspace name")
    parser.add_argument("workspace_bucket", type=str, help="Workspace bucket")
    parser.add_argument("commercial_data_bucket", type=str, help="Commercial data bucket")
    parser.add_argument("pulsar_url", type=str, help="Pulsar URL")
    parser.add_argument(
        "--catalogue_dirs",
        nargs="+",
        required=True,
        help="List of catalogue directories",
    )
    args = parser.parse_args()

    main(
        args.workspace,
        args.workspace_bucket,
        args.commercial_data_bucket,
        args.pulsar_url,
        args.catalogue_dirs,
    )
