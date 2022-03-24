import json

from google.cloud.secretmanager import SecretManagerServiceClient
from requests.auth import HTTPBasicAuth

from base_logger import log_info


def api_call_headers(api_key: str):
    log_info(__name__).info("Building API headers")
    return {"Authorization": "Basic " + api_key, "Content-Type": "application/json"}


def autheincate(api_key: str, api_secret: str):
    log_info(__name__).info("Authenticating API request")
    auth = HTTPBasicAuth(api_key, api_secret)
    return auth


def get_fivetran_configs(
    project_id="759965731268", secret_id=None, version_id="latest"
):
    log_info(__name__).info(f"Fetching Fivetran configs for {secret_id}")
    secrets = []

    sm_client = SecretManagerServiceClient()
    # config_path = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    config_path = sm_client.secret_path(project_id, secret_id)

    for version in sm_client.list_secret_versions(parent=config_path):
        response = sm_client.access_secret_version(name=version.name)
        try:
            payload = response.payload.data.decode("UTF-8")
            dictionised_payload = json.loads(payload)
        except:
            payload = response.payload.data.decode("UTF-8").replace("\n", ",")
            dictionised_payload = json.loads(payload)

        secrets.append(dictionised_payload)

    # We are only interested in comparing recently updated two secret versions for drift detection

    if len(secrets) == 1:
        log_info(__name__).info(f"No newer version found for {secret_id}")
        return secrets[0]  # ('CREATE',secrets[0])
    else:
        log_info(__name__).info(f"Newer version found for {secret_id}")
        log_info(__name__).info(f"Treating it as a modification request")
        return secrets[0]  # ('MODIFY',secrets[0])


def get_object_detail(verification_obj, object_type, object_name, object_config_dict):
    verification = verification_obj(object_type, object_name, object_config_dict)
    valid_response = ""
    requested_data = ""
    search_in_object_type = object_type.split("/")[-1]

    while valid_response != "Data_Found" or not valid_response:
        request_str = verification.build(requested_data)
        response = verification.excute(request_str)
        valid_response, requested_data = verification.search_object_by_name(
            response, search_in_object_type
        )
        log_info(__name__).info(f"{search_in_object_type}_id - {requested_data}")
        if valid_response == "Data_Found":
            return requested_data
        else:
            if valid_response == "List_Exhausted":
                log_info(__name__).info("List_Exhausted")
                break
            else:
                requested_data = requested_data


def log_known_err_and_exit(name: str, msg: str):
    log_info(name).error(f"{msg}")
    raise SystemExit
