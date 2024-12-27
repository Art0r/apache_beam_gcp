"""
Setting up env vars to be used in the main script
"""
import os
from google.cloud import secretmanager


def access_secret_version(project_id, secret_id, version_id="latest") -> str:
    client = secretmanager.SecretManagerServiceClient()

    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    response = client.access_secret_version(request={"name": name})

    return response.payload.data.decode('UTF-8')


def find_value_for_keys(*keys) -> dict[str, str]:

    keys_values_dict: dict[str, str] = {}

    keys_pairs = [key_pair for key_pair in access_secret_version(project_id="curso-apache-beam-gcp",
                                                                 secret_id="psql-bq-dataflow").split('\n')]

    for key in keys:

        filtered_key_pair = list(
            filter(lambda key_pair: key_pair.__contains__(key), keys_pairs))

        if len(filtered_key_pair) > 0:

            # se um resultado para o filtro foi encontrado
            # entao o valor encontrado, isso é, filtered_key_pair[0] (KEY=VALUE)
            # sera usado como par chave e valor, e para tanto sera .split('=')
            # onde [0] é a chave e [1] é o valor
            keys_values_dict[filtered_key_pair[0].split(
                '=')[0]] = filtered_key_pair[0].split('=')[1]

    return keys_values_dict


def assign_pair_key_value_to_env(key_value: str) -> None:
    splitted_key_value = key_value.split('=')

    key = splitted_key_value[0]
    value = splitted_key_value[1]

    os.environ[key] = value


def init_env_vars() -> None:

    if os.environ.get('GOOGLE_CLOUD_PROJECT') is None:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "curso-apache-beam-gcp.json"
