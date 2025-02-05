import logging
import os
import sys
from typing import List

from airbus_optical_adaptor.api_utils import post_submit_order
from common.s3_utils import download_and_store_locally, poll_s3_for_data
from common.stac_utils import (
    OrderStatus,
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
        self.multi_acquisition_ids = (
            get_key_from_stac(
                self.stac_json, "properties.composed_of_acquisition_identifiers"
            )
            or []
        )
        self.order_status = get_key_from_stac(self.stac_json, "order.status")
        self.item_uuids = []


def prepare_stac_items_to_order(catalogue_dirs: List[str]) -> List[STACItem]:
    """Prepare a list of STAC items to order, including multi-acquisition items"""
    stac_item_paths = []
    for catalogue_dir in catalogue_dirs:
        if not os.path.exists(catalogue_dir):
            raise FileNotFoundError(f"Catalogue directory {catalogue_dir} not found.")
        stac_item_paths += get_item_hrefs_from_catalogue(catalogue_dir)
    if not stac_item_paths:
        raise ValueError("No STAC items found in the given directories.")
    acquisition_id_to_path = {
        os.path.splitext(os.path.basename(path))[0]: path for path in stac_item_paths
    }
    logging.info(f"STAC item paths: {stac_item_paths}")

    stac_items = []

    for stac_item_path in stac_item_paths:
        stac_item_to_add = STACItem(stac_item_path)
        # Do not add the item if it is already part of a multi-acquisition item
        if any(
            stac_item_to_add.acquisition_id in stac_item.multi_acquisition_ids
            for stac_item in stac_items
        ):
            continue
        if stac_item_to_add.multi_acquisition_ids:
            logging.info(
                f"Item {stac_item_to_add.acquisition_id} is a multi-acquisition item"
            )
            logging.info(
                f"Multi-acquisition IDs: {stac_item_to_add.multi_acquisition_ids}"
            )
            for multi_acquisition_id in stac_item_to_add.multi_acquisition_ids:
                # The order is incomplete if not all multi-acquisition items are present
                multi_stac_item_path = acquisition_id_to_path.get(multi_acquisition_id)
                if not multi_stac_item_path:
                    raise ValueError(
                        f"File {multi_acquisition_id} not found in given ids: {acquisition_id_to_path}"
                    )
                # Add the UUID of each item to the main multi-acquisition item
                multi_stac_item = STACItem(multi_stac_item_path)
                stac_item_to_add.item_uuids.append(
                    get_key_from_stac(multi_stac_item.stac_json, "properties.id")
                )
            # Remove the multi-acquisition items from the list
            stac_items = [
                item
                for item in stac_items
                if item.acquisition_id not in stac_item_to_add.multi_acquisition_ids
            ]
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
        "radiometricProcessing": "reflectance",
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
            if stac_item.order_status == OrderStatus.ORDERED.value:
                logging.info(
                    f"Order for {stac_item.acquisition_id} is already in progress"
                )
                # Unable to obtain the item_id again, so cannot wait for data. Fail the order.
                update_stac_item_failure(stac_item.stac_json, stac_item.file_name)
                return
            order_id = post_submit_order(
                stac_item.acquisition_id,
                stac_item.collection_id,
                stac_item.coordinates,
                order_options,
                stac_item.item_uuids,
            )
        except Exception as e:
            logging.error(f"Failed to submit order: {e}")
            update_stac_item_failure(stac_item.stac_json, stac_item.file_name)
            return
        try:
            # Wait for data from airbus to arrive, then download it
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
