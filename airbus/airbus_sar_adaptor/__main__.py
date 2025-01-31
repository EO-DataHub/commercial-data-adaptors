import logging
import sys

from airbus_sar_adaptor.api_utils import is_order_in_progress, post_submit_order
from common.s3_utils import poll_s3_for_data, retrieve_stac_item, unzip_and_upload_to_s3
from common.stac_utils import (
    get_acquisition_id_from_stac,
    update_stac_item_failure,
    update_stac_item_success,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def main(stac_key: str, workspace_bucket: str, workspace_domain: str, pulsar_url: str, env="dev"):
    """Submit an order for an acquisition, retrieve the data, and update the STAC item"""
    # Workspace STAC item should already be generated and ingested, with an order status of ordered.
    stac_parent_folder = "/".join(stac_key.split("/")[:-1])
    try:
        # Submit an order for the given STAC item
        logging.info(f"Retrieving STAC item {stac_key} from bucket {workspace_bucket}")
        stac_item = retrieve_stac_item(workspace_bucket, stac_key)
        acquisition_id = get_acquisition_id_from_stac(stac_item, stac_key)
        if is_order_in_progress(acquisition_id, env):
            logging.info(f"Order for {acquisition_id} is already in progress")
            # TODO: Check if the order in progress is for the exact same item
            update_stac_item_failure(workspace_bucket, stac_key, None)
            return
        item_id = post_submit_order(acquisition_id, env)
    except Exception as e:
        logging.error(f"Failed to submit order: {e}")
        update_stac_item_failure(workspace_bucket, stac_key, None)
        return
    try:
        # Wait for data from airbus to arrive, then move it to the workspace
        obj = poll_s3_for_data("commercial-data-airbus", item_id)
        unzip_and_upload_to_s3(
            "commercial-data-airbus",
            workspace_bucket,
            f"{stac_parent_folder}/{item_id}",
            obj,
        )
    except Exception as e:
        logging.error(f"Failed to retrieve data: {e}")
        update_stac_item_failure(workspace_bucket, stac_key, item_id)
        return
    update_stac_item_success(
        workspace_bucket, stac_key, stac_parent_folder, item_id, workspace_domain, pulsar_url
    )


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[5], sys.argv[4])
