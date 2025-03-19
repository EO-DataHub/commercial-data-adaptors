import argparse
import json
import logging
import os
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
        self.acquisition_id = get_key_from_stac(self.stac_json, "id")
        self.collection_id = get_key_from_stac(self.stac_json, "collection")
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


def get_order_options(
    product_type: str, orbit: str, resolution: str, map_projection: str
) -> dict:
    """Return the order options for the given product bundle"""
    available_types = ["SSC", "MGD", "GEC", "EEC"]
    available_orbits = ["rapid", "science"]
    available_resolutions = ["RE", "SE"]
    available_map_projections = ["auto", "UTM", "UPS"]

    order_details = {"orderTemplate": "Ordering"}

    if product_type not in available_types:
        raise NotImplementedError(
            f"Product bundle {product_type} is not valid. Currently implemented bundles are {available_types}"
        )
    else:
        order_details["productType"] = product_type

    if orbit not in available_orbits:
        raise NotImplementedError(
            f"Orbit {orbit} is not valid. Currently implemented orbits are {available_orbits}"
        )
    else:
        order_details["orbit"] = orbit

    if resolution not in available_resolutions and resolution is not None:
        raise NotImplementedError(
            f"Resolution {resolution} is not valid. Currently implemented resolutions are {available_resolutions}"
        )
    else:
        order_details["resolution"] = resolution

    if map_projection not in available_map_projections and map_projection is not None:
        raise NotImplementedError(
            f"Map projection {resolution} is not valid. Currently implemented map projections are {available_map_projections}"
        )
    else:
        order_details["mapProjection"] = map_projection

    return order_details


def main(
    commercial_data_bucket: str,
    product_bundle: str,
    coordinates: List,
    catalogue_dirs: List[str],
    workspace: str,
):
    product_bundle = json.loads(product_bundle)

    """Submit an order for an acquisition, retrieve the data, and update the STAC item"""
    # Workspace STAC item should already be generated and ingested, with an order status of ordered.
    logging.info(f"Ordering items in catalogues from stage in: {catalogue_dirs}")
    order_options = get_order_options(
        product_type=product_bundle.get("product_type"),
        map_projection=product_bundle.get("projection"),
        orbit=product_bundle.get("orbit"),
        resolution=product_bundle.get("resolution"),
    )
    logging.info(f"Order options: {order_options}")
    stac_items: List[STACItem] = prepare_stac_items_to_order(catalogue_dirs)

    for stac_item in stac_items:
        try:
            # Submit an order for the given STAC item
            logging.info(f"Ordering STAC item {stac_item.acquisition_id}")
            if is_order_in_progress(stac_item.acquisition_id, workspace):
                logging.info(
                    f"Order for {stac_item.acquisition_id} is already in progress"
                )
                update_stac_item_failure(stac_item.stac_json, stac_item.file_name)
                return
            order_id = post_submit_order(
                stac_item.acquisition_id, order_options, workspace
            )
            order_id = order_id.split("_")[0]
        except Exception as e:
            logging.error(f"Failed to submit order: {e}", exc_info=True)
            update_stac_item_failure(stac_item.stac_json, stac_item.file_name)
            return
        try:
            # Wait for data from airbus to arrive, then move it to the workspace
            # Archive is of the format SO_<order_id>_<item_number>_1.tar.gz
            objs = poll_s3_for_data(commercial_data_bucket, f"SO_{order_id}", ".tar.gz")
            for obj in objs:
                download_and_store_locally(
                    commercial_data_bucket,
                    obj,
                    "assets",
                )
        except Exception as e:
            logging.error(f"Failed to retrieve data: {e}", exc_info=True)
            update_stac_item_failure(stac_item.stac_json, stac_item.file_name, order_id)
            return
        update_stac_item_success(
            stac_item.stac_json, stac_item.file_name, order_id, "assets"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Order Airbus data")
    parser.add_argument(
        "commercial_data_bucket", type=str, help="Commercial data bucket"
    )
    parser.add_argument("product_bundle", type=str, help="Product bundle")
    parser.add_argument(
        "--coordinates", type=str, required=True, help="Stringified list of coordinates"
    )
    parser.add_argument(
        "--catalogue_dirs",
        nargs="+",
        required=True,
        help="List of catalogue directories",
    )
    parser.add_argument(
        "--workspace",
        type=str,
        required=True,
        help="Target workspace the order will be sent to",
    )

    args = parser.parse_args()

    coordinates = json.loads(args.coordinates)

    main(
        args.commercial_data_bucket,
        args.product_bundle,
        coordinates,
        args.catalogue_dirs,
        args.workspace,
    )
