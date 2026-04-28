import argparse
import logging
import os
from pathlib import Path

import requests

from open_cosmos_adaptor.auth_utils import get_access_token, get_contract_info
from s3_utils import retrieve_stac_item, poll_s3_for_data, download_and_store_locally
from stac_utils import get_key_from_stac, get_item_hrefs_from_catalogue, update_stac_item_failure, \
    update_stac_item_success, update_stac_item_ordered

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

products = [
    "hammer-l1c-cogs",
    "mantis-l1d-cogs",
    "menut-l1a-cogs",
    "menut-l1b-cogs",
    "menut-l1c-cogs",
    "platero-l1c-cogs",
]

class STACItem:
    """Class to represent a STAC item and its properties"""

    def __init__(self, stac_item_path: str) -> None:
        self.file_path = stac_item_path
        self.file_name = os.path.basename(stac_item_path)
        self.stac_json = retrieve_stac_item(stac_item_path)
        self.item_id = get_key_from_stac(self.stac_json, "id")
        self.collection_id = get_key_from_stac(self.stac_json, "collection")
        self.processing_level = get_key_from_stac(self.stac_json, "processing:level")


def prepare_stac_items_to_order(catalogue_dirs: list[str]) -> list[STACItem]:
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


def create_order_request(collection_id: str, item_id: str, level: str, organisation_id: str, contract_id: str) -> dict:
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
    stac_items: list[STACItem] = prepare_stac_items_to_order(catalogue_dirs)
    contract_info = get_contract_info()

    for stac_item in stac_items:
        collection_id = stac_item.collection_id
        if collection_id not in products:
            raise NotImplementedError(
                f"Collection {collection_id} is not valid. Currently implemented collections are: "
                f"{', '.join(products)}"
            )

        order_name = f"{stac_item.item_id}-{workspace}"

        delivery_folder = Path("opencosmos/commercial-data/orders")

        try:
            # Submit an order for the given STAC item
            logging.info(f"Ordering stac item {stac_item.item_id} in {collection_id}")

            order = create_order_request(
                collection_id,
                stac_item.item_id,
                stac_item.processing_level,
                contract_info.organisation_id,
                contract_info.contract_id
            )

            order_id = order["data"].get("id")
            if order_id is None:
                raise ValueError(f"No order ID found for order {order_name}")

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
            # Wait for data from Open Cosmos to arrive, then move it to the workspace
            poll_s3_for_data(
                source_bucket=commercial_data_bucket,
                order_id=order_id,
                folder=delivery_folder,
            )

            download_and_store_locally(commercial_data_bucket, delivery_folder / order_id, Path(order_id))
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
