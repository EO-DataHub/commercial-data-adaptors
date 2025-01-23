import logging
from pulsar import Client as PulsarClient


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
        producer_name=f"airbus-sar-adaptor-{workspace}-{file_id}",
        chunking_enabled=True,
    )
    producer.send((json.dumps(output_data)).encode("utf-8"))
