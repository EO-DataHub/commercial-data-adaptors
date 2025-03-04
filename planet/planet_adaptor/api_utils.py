import base64
import boto3
import logging
import json
from kubernetes import client, config

from Crypto.Cipher import AES
import planet



def decrypt_planet_api_key(encrypted_key_b64: str, aes_key_b64: str) -> str:
    """
    Decrypts an AES-256-GCM encrypted key using the provided OTP.

    :param encrypted_key_b64: Base64 encoded encrypted key from AWS Secrets Manager.
    :param aes_key_b64: Base64 encoded AES key from Kubernetes Secret.
    :return: Decrypted plaintext key.
    """
    try:
        # Decode the AES cluster secret
        aes_key = base64.b64decode(aes_key_b64)
        if len(aes_key) != 32:
            raise ValueError("AES KEY must be 32 bytes for AES-256")

        # Decode the encrypted AWS secret
        encrypted_key = base64.b64decode(encrypted_key_b64)

        # Extract nonce (first 12 bytes) and ciphertext + tag
        nonce_size = 12  # Standard nonce size for AES-GCM
        tag_size = 16  # AES-GCM tag size is 16 bytes

        nonce = encrypted_key[:nonce_size]
        ciphertext = encrypted_key[nonce_size:-tag_size]  # Extract ciphertext
        tag = encrypted_key[-tag_size:]  # Extract tag

        # Decrypt using AES-GCM
        cipher = AES.new(aes_key, AES.MODE_GCM, nonce=nonce)
        decrypted_key = cipher.decrypt_and_verify(ciphertext, tag)

        # Decode the decrypted key
        decrypted_text = decrypted_key.decode("utf-8")

        return decrypted_text

    except UnicodeDecodeError:
        logging.error("Warning: Decrypted data is not valid UTF-8. Returning raw bytes.")
        return decrypted_key.hex()
    except ValueError as e:
        logging.error(f"Integrity check failed: {e}")
        return None
    except Exception as e:
        logging.error(f"Decryption failed: {e}")
        return None


def get_planet_api_key(
    workspace: str
) -> str:
    """
    Retrieve an OTP (one-time pad) AES key from a Kubernetes secret and use it to decrypt an AWS secret.
    
    Steps:
    1. Load Kubernetes in-cluster config and initialize the API client.
    2. Retrieve the AES key from the Kubernetes secret store.
    3. Use the AES key to decrypt an encrypted secret stored in AWS Secrets Manager.
    4. Return the decrypted API key.
    """

    provider = "planet"

    # Create a Kubernetes API client
    config.load_incluster_config()
    v1 = client.CoreV1Api()
    namespace = f"ws-{workspace}"

    # Retreive the decryption key from kubernetes secret
    logging.info("Fetching AES decryption secret from Kubernetes...")
    secret_data = v1.read_namespaced_secret(f'aes-key-{provider}', namespace)
    aes_key_encoded = secret_data.data.get('aes-key')

    if not aes_key_encoded:
        raise ValueError(f"AES encryption key not found in Kubernetes Secret in namespace {namespace}.")

    # Decode the AES encryption key
    aes_key_b64 = base64.b64decode(aes_key_encoded).decode("utf-8")
    logging.info("Successfully retrieved and decoded AES encryption key.")

    # Initialize AWS Secrets Manager client and fetch all secrets in target namespace
    logging.info(f"Fetching encrypted secret for provider {provider} from AWS Secrets Manager...")
    secrets_client = boto3.client('secretsmanager')
    response = secrets_client.get_secret_value(SecretId=namespace)

    # Extract the secret string and parse it as JSON
    secret_string = response.get("SecretString", "{}")
    secret_dict = json.loads(secret_string)
    
    # Retrieve the encrypted API key (Base64 encoded)
    encrypted_api_key_b64 = secret_dict.get(provider)
    if not encrypted_api_key_b64:
        raise ValueError("Encrypted API key not found in AWS Secrets Manager.")
    
    # Decrypt the API key using the AES encryption key
    decrypted_api_key = decrypt_planet_api_key(encrypted_api_key_b64, aes_key_b64)

    logging.info(f"Successfully fetched API key for {provider}")

    return decrypted_api_key



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

    order = planet.order_request.build_request(
        name=order_id,
        products=[
            planet.order_request.product(
                item_ids=[item_id],
                product_bundle=product_bundle,
                item_type=collection_id,
            )
        ],
        tools=[planet.order_request.clip_tool(aoi=aoi)],
        delivery=delivery,
    )

    return order


async def submit_order(workspace: str, order_details: dict) -> str:
    """Submit an order for Planet data"""
    planet_api_key = get_api_key_from_secret("api-keys", "planet-key")
    auth = planet.Auth.from_key(planet_api_key)
    async with planet.Session(auth=auth) as sess:
        # 'orders' is the service name for the Orders API.
        cl = sess.client("orders")

        order = await cl.create_order(order_details)

        return order
