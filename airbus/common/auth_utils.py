import boto3
import base64
import json
import logging
import requests
from kubernetes import client, config
from Crypto.Cipher import AES



def decrypt_secret(encrypted_key_b64: str, aes_key_b64: str) -> str:
    """
    Decrypts an AES-256-GCM encrypted key using the provided OTP.

    :param encrypted_key_b64: Base64 encoded encrypted key from AWS Secrets Manager.
    :param otp_b64: Base64 encoded OTP from Kubernetes Secret.
    :return: Decrypted plaintext key.
    """
    try:
        # Step 1: Decode OTP (AES key)
        aes_key = base64.b64decode(aes_key_b64)
        if len(aes_key) != 32:
            raise ValueError("AES KEY must be 32 bytes for AES-256")

        # Step 2: Decode the encrypted key
        encrypted_key = base64.b64decode(encrypted_key_b64)

        # Step 3: Extract nonce (first 12 bytes) and ciphertext + tag
        nonce_size = 12  # Standard nonce size for AES-GCM
        tag_size = 16  # AES-GCM tag size is 16 bytes

        nonce = encrypted_key[:nonce_size]
        ciphertext = encrypted_key[nonce_size:-tag_size]  # Extract ciphertext
        tag = encrypted_key[-tag_size:]  # Extract tag

        # Step 4: Decrypt using AES-GCM
        cipher = AES.new(aes_key, AES.MODE_GCM, nonce=nonce)
        decrypted_key = cipher.decrypt_and_verify(ciphertext, tag)  # Verify integrity

        # Step 5: Decode the decrypted key as UTF-8
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


def get_api_key_from_secret(
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
    # Create a Kubernetes API client
    config.load_incluster_config()
    v1 = client.CoreV1Api()
    namespace = f"ws-{workspace}"

    # Retreive the decryption key from kubernetes secret
    logging.info("Fetching AES decryption secret from Kubernetes...")
    secret_data = v1.read_namespaced_secret('aes-key-airbus', namespace)
    aes_key_encoded = secret_data.data.get('aes-key')

    if not aes_key_encoded:
        raise ValueError(f"AES encryption key not found in Kubernetes Secret in namespace {namespace}.")

    # Decode the AES encryption key
    aes_key = base64.b64decode(aes_key_encoded).decode("utf-8")
    logging.info("Successfully retrieved and decoded AES encryption key.")

    # Initialize AWS Secrets Manager client and fetch all secrets in target namespace
    logging.info("Fetching encrypted API key from AWS Secrets Manager...")
    secrets_client = boto3.client('secretsmanager')
    response = secrets_client.get_secret_value(SecretId=namespace)

    # Extract the secret string and parse it as JSON
    secret_string = response.get("SecretString", "{}")
    secret_dict = json.loads(secret_string)
    
    # Retrieve the encrypted API key (Base64 encoded)
    encrypted_api_key_b64 = secret_dict.get("airbus")
    if not encrypted_api_key_b64:
        raise ValueError("Encrypted API key not found in AWS Secrets Manager.")
    
    # Decrypt the API key using the AES encryption key
    decrypted_api_key = decrypt_secret(encrypted_api_key_b64, aes_key)

    logging.info(f"Successfully decrypted API key: {decrypted_api_key}")

    return decrypted_api_key


def generate_access_token(workspace: str, env: str = "prod") -> str:
    """Generate an access token for the Airbus OneAtlas API"""


    api_key = get_api_key_from_secret(workspace)
    if not api_key:
        raise ValueError("API key not found in secret")

    if env == "prod":
        url = "https://authenticate.foundation.api.oneatlas.airbus.com/auth/realms/IDP/protocol/openid-connect/token"
    else:
        url = "https://authenticate-int.idp.private.geoapi-airbusds.com/auth/realms/IDP/protocol/openid-connect/token"

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }

    data = [
        ("apikey", api_key),
        ("grant_type", "api_key"),
        ("client_id", "IDP"),
    ]

    response = requests.post(url, headers=headers, data=data)
    logging.info(f"RESPONSE: {response.text}")
    return response.json()["access_token"]
