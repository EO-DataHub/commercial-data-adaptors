import asyncio
import json
import logging
import mimetypes
import sys
from enum import Enum

from planet_adaptor.api_utils import create_order_request, define_delivery, submit_order, get_api_key_from_secret
from planet_adaptor.s3_utils import (
    list_objects_in_folder,
    poll_s3_for_data,
    retrieve_stac_item,
    unzip_and_upload_to_s3,
    upload_stac_item,
)
from planet_adaptor.stac_utils import (
    get_id_and_collection_from_stac,
    update_stac_order_status,
    write_stac_item_and_catalog,
)
import planet
from pulsar import Client as PulsarClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class OrderStatus(Enum):
    ORDERABLE = "orderable"
    ORDERED = "ordered"
    PENDING = "pending"
    SHIPPING = "shipping"
    SUCCEEDED = "succeeded"
    CANCELED = "canceled"
    FAILED = "failed"


def send_pulsar_message(bucket: str, key: str):
    """Send a Pulsar message to indicate an update to the item"""
    parts = key.split("/")
    workspace = parts[0]
    file_id = parts[-1]
    output_data = {
        "id": f"{workspace}/order_item/{file_id}",
        "workspace": workspace,
        "bucket_name": bucket,
        "added_keys": [],
        "updated_keys": [key],
        "deleted_keys": [],
        "source": workspace,
        "target": f"user-datasets/{workspace}",
    }
    logging.info(f"Sending message to pulsar: {output_data}")
    pulsar_client = PulsarClient("pulsar://pulsar-broker.pulsar:6650")
    producer = pulsar_client.create_producer(
        topic="harvested",
        producer_name=f"planet-adaptor-{workspace}-{file_id}",
        chunking_enabled=True,
    )
    producer.send((json.dumps(output_data)).encode("utf-8"))


def update_stac_item_success(
    bucket: str, key: str, parent_folder: str, item_id: str, workspaces_domain: str
):
    """Update the STAC item with the assets and success order status"""
    stac_item = retrieve_stac_item(bucket, key)
    # List files and folders in the specified folder
    folder_prefix = f"{parent_folder}/{item_id}/"
    folder_objects = list_objects_in_folder(bucket, folder_prefix)

    # Add all listed objects as assets to the STAC item
    if "Contents" in folder_objects:
        for obj in folder_objects["Contents"]:
            file_key = obj["Key"]

            # Skip if the file_key is a folder
            if file_key.endswith("/"):
                continue

            asset_name = file_key.split("/")[-1]

            # Determine the MIME type of the file
            mime_type, _ = mimetypes.guess_type(file_key)
            if mime_type is None:
                mime_type = "application/octet-stream"  # Default MIME type

            # Add asset link to the file
            parts = file_key.split("/", 1)
            workspace = parts[0]
            file_subpath = parts[1]
            stac_item["assets"][asset_name] = {
                "href": f"https://{workspace}.{workspaces_domain}/files/{bucket}/{file_subpath}",
                "type": mime_type,
            }
    # Mark the order as succeeded and upload the updated STAC item
    update_stac_order_status(stac_item, item_id, OrderStatus.SUCCEEDED.value)
    upload_stac_item(bucket, key, stac_item)
    send_pulsar_message(bucket, key)

    # Create local record of the order, to be used as the workflow output
    write_stac_item_and_catalog(stac_item, key.split("/")[-1], item_id)


def update_stac_item_failure(bucket: str, key: str, item_id: str):
    """Update the STAC item with the failure order status"""
    stac_item = retrieve_stac_item(bucket, key)

    # Mark the order as failed and upload the updated STAC item
    update_stac_order_status(stac_item, item_id, OrderStatus.FAILED.value)
    upload_stac_item(bucket, key, stac_item)
    send_pulsar_message(bucket, key)

    # Create local record of attempted order, to be used as the workflow output
    write_stac_item_and_catalog(stac_item, key.split("/")[-1], item_id)


def generate_request(item_id, collection, credentials, bucket, region="eu-west-2"):
    return {
       "name": "amazon_s3_delivery_order",
       "products": [
          {
             "item_ids": [
                item_id
             ],
             "item_type": collection,
             "product_bundle":"analytic_udm2"
          }
       ],
       "delivery": {
          "amazon_s3": {
             "bucket": bucket,
             "aws_region": region,
             "aws_access_key_id": credentials['AccessKeyId'],
             "aws_secret_access_key": credentials['SecretAccessKey'],
             "path_prefix": "planet/"
          }
       }
    }

async def get_existing_order_details(item_id):
    planet_api_key = get_api_key_from_secret("api-keys", "planet-key")
    auth = planet.Auth.from_key(planet_api_key)

    session = planet.Session(auth=auth)
    orders_client = planet.OrdersClient(session=session)

    async for order in orders_client.list_orders():
        for product in order['products']:
            for product_item_id in product['item_ids']:
                if product_item_id == item_id:
                    return order
                # return order['id'], order['state']

    return {}


def get_credentials() -> dict:

    return {"AccessKeyId": get_api_key_from_secret("planet-aws-access-key-id", "planet-aws-access-key-id"),
            "SecretAccessKey": get_api_key_from_secret("planet-aws-secret-access-key", "planet-aws-secret-access-key")
            }


def main(stac_key: str, workspace_bucket: str, workspace_domain: str):
    """Submit an order for an acquisition, retrieve the data, and update the STAC item"""
    # Workspace STAC item should already be generated and ingested, with an order status of ordered.
    stac_parent_folder = "/".join(stac_key.split("/")[:-1])
    planet_data_bucket = "commercial-planet-data"
    try:
        # Submit an order for the given STAC item
        logging.info(f"Retrieving STAC item {stac_key} from bucket {workspace_bucket}")
        stac_item = retrieve_stac_item(workspace_bucket, stac_key)

        item_id, collection_id = get_id_and_collection_from_stac(stac_item, stac_key)

        logging.info(f"Preparing to submit order for {item_id} in {collection_id}")

        order = asyncio.run(get_existing_order_details(item_id))

        order_status = order.get('state')
        logging.info(f"Order status: {order_status}")
        if order_status in ["queued", "running"]:
            order_id = order.get('id')
            logging.info(f"Order for {item_id} has already been submitted: {order_id}")
            update_stac_item_failure(workspace_bucket, stac_key, None)
            return

        if not order_status == "success":
            credentials = get_credentials()

            delivery_request = define_delivery(credentials, planet_data_bucket)
            order_request = create_order_request(item_id, collection_id, delivery_request)

            asyncio.run(submit_order(order_request))

            order = asyncio.run(get_existing_order_details(item_id))

        order_id = order.get('id')
        logging.info(f"Found order ID {order_id}")

    except Exception as e:
        logging.error(f"Failed to submit order: {e}")
        update_stac_item_failure(workspace_bucket, stac_key, None)
        return

    try:
        # Wait for data from planet to arrive, then move it to the workspace
        poll_s3_for_data(source_bucket=planet_data_bucket, order_id=order_id, item_id=item_id)

        unzip_and_upload_to_s3(
            "commercial-planet-data",
            workspace_bucket,
            f"{stac_parent_folder}/{order_id}",
            order_id,
            item_id
        )
    except Exception as e:
        logging.error(f"Failed to retrieve data: {e}")
        update_stac_item_failure(workspace_bucket, stac_key, item_id)
        return
    update_stac_item_success(
        workspace_bucket, stac_key, stac_parent_folder, item_id, workspace_domain
    )


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])
