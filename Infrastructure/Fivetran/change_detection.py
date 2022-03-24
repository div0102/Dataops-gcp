from google.cloud import storage

from base_logger import log_info
from utils import get_fivetran_configs, get_object_detail

"""
Role of this module is to compare current payload with previous payload and identify

whether its a creation or modification request and same or modified payload

"""


def check_storage(config_bucket_name: str = "", config_file_nm: str = ""):
    gcs_client = storage.Client()
    config_bucket = gcs_client.get_bucket(bucket_or_name=config_bucket_name)
    file_attr = config_bucket.blob(config_file_nm)
    if file_attr.exists():
        log_info(__name__).info("Config file found")
        # Assumption if multiple versions of a file exist then its a modification request.
        # In such cases we return back the latest file object
        if file_attr.encryption_key():
            return file_attr.download_as_string().decode("UTF-8")
        else:
            log_info(__name__).info("File modification detected")
            return file_attr.download_as_string().decode("UTF-8")
    else:
        # If a config is not present for particular request then proceeds with the setting defined in input
        # or in case of sensitive information what is being defined secret manager
        log_info(__name__).info(
            "Config file not found. Proceeding with default settings"
        )


def change_detection(config_name: str):
    pass


print(
    check_storage(
        config_bucket_name="fivetran-configs",
        config_file_nm="test.txt",
    )
)
