"""
Setting up env vars to be used in the main script
"""
import os
from google.cloud import secretmanager


def access_secret_version(project_id, secret_id, version_id="latest") -> str:
    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version.
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version.
    response = client.access_secret_version(request={"name": name})

    # Return the decoded payload.
    return response.payload.data.decode('UTF-8')


def assign_pair_key_value_to_env(key_value: str) -> None:
    splitted_key_value = key_value.split('=')

    key = splitted_key_value[0]
    value = splitted_key_value[1]

    os.environ[key] = value
