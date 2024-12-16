import base64
from kubernetes import client, config
import planet


def get_api_key_from_secret(
    secret_name: str, secret_key: str, namespace: str = "ws-planet"
) -> str:
    """Retrieve an API key from a Kubernetes secret"""
    # Create a Kubernetes API client
    config.load_incluster_config()
    v1 = client.CoreV1Api()

    # Retrieve and decode the secret
    secret = v1.read_namespaced_secret(secret_name, namespace)
    api_key_base64 = secret.data[secret_key]
    api_key = base64.b64decode(api_key_base64).decode("utf-8")

    return api_key


def define_delivery(credentials: dict, bucket: str) -> dict:
    return planet.order_request.amazon_s3(
        credentials["AccessKeyId"],
        credentials["SecretAccessKey"],
        bucket,
        "eu-west-2",
        path_prefix="planet",
    )


def create_order_request(item_id: str, collection_id: str, delivery: dict) -> dict:
    """Create an order for Planet data"""

    order = planet.order_request.build_request(
        name=item_id,
        products=[
            planet.order_request.product(
                item_ids=[item_id],
                product_bundle="analytic_udm2",
                item_type=collection_id,
            )
        ],
        delivery=delivery,
    )

    return order


async def submit_order(order_details) -> str:
    """Submit an order for Planet data"""

    auth = planet.Auth.from_env(variable_name="PLANET_API_KEY")
    async with planet.Session(auth=auth) as sess:

        # 'orders' is the service name for the Orders API.
        cl = sess.client("orders")

        order = await cl.create_order(order_details)

        return order
