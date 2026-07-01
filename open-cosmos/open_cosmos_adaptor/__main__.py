import argparse
import logging
import os
from pathlib import Path

import requests
from pystac import Item

from open_cosmos_adaptor.auth_utils import get_access_token, get_contract_info

from .s3_utils import download_and_store_locally, upload_to_s3
from .stac_utils import (
    get_item_hrefs_from_catalogue,
    update_stac_item_failure,
    update_stac_item_ordered,
    update_stac_item_success,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def prepare_stac_items_to_order(catalogue_dirs: list[str]) -> dict[str, Item]:
    """Loads any STAC catalogues in `catalogue_dirs` and returns a
    dictionary of STAC items present in those catalogues."""

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


def create_order_request(
        workspace: str, collection_id: str, item_id: str, processing_level: str, organisation_id: int, contract_id: int
) -> dict:
    """Builds an order payload and submits it to the Open Cosmos API.
    See: https://app.open-cosmos.com/help/developer-center/datacosmos/api/ordering/purchasing

    :returns: The response from the API.
    """

    url = "https://app.open-cosmos.com/api/data/v0/order/orders"

    order = {
        "type": "IMAGE",
        "data": {"order_line_items": [{"collection": collection_id, "item": item_id, "level": processing_level}]},
        "organisation": organisation_id,
        "contract_id": contract_id,
    }

    headers = {"Authorization": f"Bearer {get_access_token(workspace)}"}

    logging.debug(f"Sending order request to {url} with headers {headers} and body of {order}")
    r = requests.post(url, json=order, headers=headers)
    r.raise_for_status()

    return r.json()


def main(
        workspace: str,
        workspace_bucket: str,
        pulsar_url: str,
        catalogue_dirs: list[str],
) -> None:
    logging.info(f"Preparing Open Cosmos data for {workspace} for the following: {catalogue_dirs}")
    new_stac_items: dict[str, Item] = prepare_stac_items_to_order(catalogue_dirs)
    contract_info = get_contract_info(workspace)

    for file_name, stac_item in new_stac_items.items():
        collection_id = stac_item.collection_id
        if collection_id is None:
            raise ValueError(f"Collection ID is None for item {stac_item.id}")

        order_name = f"{stac_item.id}-{workspace}"

        # Submit an order for the given STAC item
        logging.info(f"Ordering stac item {stac_item.id} in {collection_id}")

        try:
            order = create_order_request(
                workspace,
                collection_id,
                stac_item.id,
                stac_item.properties["processing:level"],
                contract_info.organisation_id,
                contract_info.contract_id,
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
            download_and_store_locally(workspace, stac_item, Path(order_id))
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

        try:
            upload_to_s3(stac_item, Path(order_id), workspace_bucket, f"{workspace}/commercial-data/open-cosmos/{stac_item.collection_id}/{stac_item.id}/")
        except Exception as e:
            reason = f"Failed to upload data: {e}"
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
    parser.add_argument("pulsar_url", type=str, help="Pulsar URL")
    parser.add_argument(
        "--catalogue_dirs",
        nargs="+",
        required=True,
        help="List of catalogue directories",
    )
    args = parser.parse_known_args()[0]

    main(
        args.workspace,
        args.workspace_bucket,
        args.pulsar_url,
        args.catalogue_dirs,
    )
