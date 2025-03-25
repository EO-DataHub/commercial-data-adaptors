import base64
import json
import logging

import boto3
from kubernetes import client, config

import planet


def decrypt_planet_api_key(ciphertext_b64: str, otp_key_b64: str) -> str:
    """
    Decrypts a ciphertext using One-Time Pad (OTP) via XOR.

    :param ciphertext_b64: Base64 encoded ciphertext from AWS Secrets Manager.
    :param otp_key_b64: Base64 encoded OTP key from Kubernetes Secret.
    :return: Decrypted plaintext API key.
    """

    try:
        # Decode both OTP key and ciphertext from Base64
        ciphertext = base64.b64decode(ciphertext_b64)
        otp_key = base64.b64decode(otp_key_b64)

        if len(ciphertext) != len(otp_key):
            raise ValueError("Ciphertext and OTP key must be the same length.")

        # XOR decryption
        plaintext_bytes = bytes(c ^ k for c, k in zip(ciphertext, otp_key))

        return plaintext_bytes.decode("utf-8")

    except UnicodeDecodeError:
        logging.error(
            "Warning: Decrypted data is not valid UTF-8. Returning raw bytes."
        )
        return plaintext_bytes.hex()
    except ValueError as e:
        logging.error(f"Integrity check failed: {e}")
        return None
    except Exception as e:
        logging.error(f"Decryption failed: {e}")
        return None


def get_planet_api_key(workspace: str) -> str:
    """
    Retrieve an OTP (One-Time Pad) from Kubernetes Secrets and use it to decrypt
    an encrypted API key stored in AWS Secrets Manager.

    Steps:
    1. Load Kubernetes config and initialize the API client.
    2. Retrieve the OTP key from Kubernetes secret.
    3. Retrieve the ciphertext from AWS Secrets Manager.
    4. Use the OTP key to decrypt the ciphertext and return the plaintext API key.
    """

    provider = "planet"

    # Initialize Kubernetes API client
    config.load_incluster_config()
    v1 = client.CoreV1Api()
    namespace = f"ws-{workspace}"

    # Retrieve the OTP key from Kubernetes Secrets
    logging.info("Fetching OTP key from Kubernetes...")
    secret_data = v1.read_namespaced_secret(f"otp-{provider}", namespace)
    otp_key_b64 = secret_data.data.get("otp")  # Adjusted key name for OTP

    if not otp_key_b64:
        raise ValueError(
            f"OTP key not found in Kubernetes Secret in namespace {namespace}."
        )

    # Initialize AWS Secrets Manager client and fetch the provider's ciphertext
    logging.info(
        f"Fetching ciphertext for provider '{provider}' from AWS Secrets Manager..."
    )
    secrets_client = boto3.client("secretsmanager")
    response = secrets_client.get_secret_value(SecretId=namespace)

    # Extract the secret string and parse it as JSON
    secret_string = response.get("SecretString", "{}")
    secret_dict = json.loads(secret_string)

    # Retrieve the encrypted API key (Base64 encoded ciphertext)
    ciphertext_b64 = secret_dict.get(provider)
    if not ciphertext_b64:
        raise ValueError(
            f"Ciphertext (encrypted API key) not found in AWS Secrets Manager for provider {provider}."
        )

    # Decrypt the API key using the OTP key
    plaintext_api_key = decrypt_planet_api_key(ciphertext_b64, otp_key_b64)

    logging.info(f"Successfully fetched API key for {provider}")

    return plaintext_api_key


def get_aws_api_key_from_secret(
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


def define_delivery(credentials: dict, bucket: str, folder: str) -> dict:
    """Define the delivery settings for a Planet order"""
    return planet.order_request.amazon_s3(
        credentials["AccessKeyId"],
        credentials["SecretAccessKey"],
        bucket,
        "eu-west-2",
        path_prefix=folder,
    )


def create_order_request(
    order_id: str,
    item_id: str,
    collection_id: str,
    delivery: dict,
    product_bundle: str,
    coordinates: list,
) -> dict:
    """Create an order for Planet data"""

    aoi = {
        "type": "Polygon",
        "coordinates": coordinates,
    }

    product_bundles = product_bundle.split(",")
    if len(product_bundles) == 2:
        products = [
            planet.order_request.product(
                item_ids=[item_id],
                product_bundle=product_bundles[0],
                fallback_bundle=product_bundles[1],
                item_type=collection_id,
            )
        ]
    else:
        products = [
            planet.order_request.product(
                item_ids=[item_id],
                product_bundle=product_bundle,
                item_type=collection_id,
            )
        ]

    order = planet.order_request.build_request(
        name=order_id,
        products=products,
        tools=[planet.order_request.clip_tool(aoi=aoi)],
        delivery=delivery,
    )

    return order


async def submit_order(workspace: str, order_details: dict) -> str:
    """Submit an order for Planet data"""
    planet_api_key = get_planet_api_key(workspace)
    auth = planet.Auth.from_key(planet_api_key)
    async with planet.Session(auth=auth) as sess:
        # 'orders' is the service name for the Orders API.
        cl = sess.client("orders")

        order = await cl.create_order(order_details)

        return order
