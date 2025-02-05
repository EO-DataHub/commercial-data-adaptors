import logging
import os
import sys
from typing import List

from airbus_sar_adaptor.api_utils import is_order_in_progress, post_submit_order
from common.s3_utils import download_and_store_locally, poll_s3_for_data
from common.stac_utils import (
    get_item_hrefs_from_catalogue,
    get_key_from_stac,
    retrieve_stac_item,
    update_stac_item_failure,
    update_stac_item_success,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class STACItem:
    """Class to represent a STAC item and its properties"""

    def __init__(self, stac_item_path: str):
        self.file_path = stac_item_path
        self.file_name = os.path.basename(stac_item_path)
        self.stac_json = retrieve_stac_item(stac_item_path)
        self.acquisition_id = get_key_from_stac(
            self.stac_json, "properties.acquisition_identifier"
        )
        self.collection_id = get_key_from_stac(self.stac_json, "collection")
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


def get_order_options(product_bundle: str) -> dict:
    """Return the order options for the given product bundle"""
    # TODO: Expand and implement different options based on product bundle
    available_bundles = ["general_use"]
    if product_bundle not in available_bundles:
        raise NotImplementedError(
            f"Product bundle {product_bundle} is not valid. Currently implemented bundles are {available_bundles}"
        )
    return {
        "productBundle": product_bundle,
        "orderTemplate": "Single User License",
    }


def main(commercial_data_bucket: str, product_bundle: str, catalogue_dirs: List[str]):
    """Submit an order for an acquisition, retrieve the data, and update the STAC item"""
    # Workspace STAC item should already be generated and ingested, with an order status of ordered.
    logging.info(f"Ordering items in catalogues from stage in: {catalogue_dirs}")
    order_options = get_order_options(product_bundle)
    logging.info(f"Order options: {order_options}")
    stac_items: List[STACItem] = prepare_stac_items_to_order(catalogue_dirs)

    for stac_item in stac_items:
        try:
            # Submit an order for the given STAC item
            logging.info(f"Ordering STAC item {stac_item.acquisition_id}")
            if is_order_in_progress(stac_item.acquisition_id):
                logging.info(
                    f"Order for {stac_item.acquisition_id} is already in progress"
                )
                update_stac_item_failure(stac_item.stac_json, stac_item.file_name)
                return
            order_id = post_submit_order(stac_item.acquisition_id, product_bundle)
        except Exception as e:
            logging.error(f"Failed to submit order: {e}")
            update_stac_item_failure(stac_item.stac_json, stac_item.file_name)
            return
        try:
            # Wait for data from airbus to arrive, then move it to the workspace
            obj = poll_s3_for_data(commercial_data_bucket, order_id)
            download_and_store_locally(
                commercial_data_bucket,
                obj,
                "assets",
            )
        except Exception as e:
            logging.error(f"Failed to retrieve data: {e}")
            update_stac_item_failure(stac_item.stac_json, stac_item.file_name, order_id)
            return
        update_stac_item_success(
            stac_item.stac_json, stac_item.file_name, order_id, "assets"
        )


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3:])
