import json
from abc import ABC, abstractmethod

from base_logger import log_info
from utils import get_fivetran_configs, get_object_detail, log_known_err_and_exit
from verify_base import verfiyFivetranObj


class payloadBuilder(ABC):
    """Base class for building different request payload"""

    HISTORICAL_SYNC = False
    TRUST_CERTIFICATES = True
    TRUST_FINGERPRINTS = True
    TEST_CONNECTOR = True
    IS_CONNECTOR_PAUSED = True
    REGION = "US"

    @abstractmethod
    def build_payload(self) -> dict:
        """Builds payload for specific request type"""


class create_warehouse_payload_builder(payloadBuilder):
    def build_payload(self, warehouse_name: dict, fivetran_configs: dict) -> dict:
        log_info(__name__).info(f"Buildig payload for group request")
        if not warehouse_name["name"]:
            error_msg = "Cannot create a warehouse without a name"
            log_known_err_and_exit(__name__, error_msg)

        payload = {"name": warehouse_name["name"]}
        return payload


class create_connector_payload_builder(payloadBuilder):
    def build_payload(self, connector_vals: dict, fivetran_configs: dict) -> dict:

        log_info(__name__).info(f"Buildig payload for connector request")

        if not connector_vals["service"]:
            error_msg = "Cannot create connector without a type of service"
            log_known_err_and_exit(__name__, error_msg)

        # fivetran_configs = get_fivetran_configs(secret_id="fivetran-global")
        group_id = get_object_detail(
            verfiyFivetranObj, "groups", connector_vals["group_name"], fivetran_configs
        )

        if not group_id:
            error_msg = "Cannot create connector without group id"
            log_known_err_and_exit(__name__, error_msg)

        config = get_fivetran_configs(secret_id=connector_vals["configs"])
        if not config:
            error_msg = "Cannot create connector without a type of defining its configs"
            log_known_err_and_exit(__name__, error_msg)

        payload = {
            "service": connector_vals["service"],
            "group_id": group_id,
            "paused": self.IS_CONNECTOR_PAUSED,
            "is_historical_sync": False,
            "sync_frequency": 60,
            "trust_certificates": self.TRUST_CERTIFICATES,
            "trust_fingerprints": self.TRUST_FINGERPRINTS,
            "run_setup_tests": self.TEST_CONNECTOR,  # set
            "config": config,
            "api_paste_format": "python",
            "schedule_type": "manual",
        }
        return payload


class create_destination_payload_builder(payloadBuilder):
    def build_payload(self, connector_vals: dict, fivetran_configs: dict) -> dict:

        log_info(__name__).info(f"Buildig payload for destination request")

        if not connector_vals["service"]:
            error_msg = "Cannot create connector without a type of service"
            log_known_err_and_exit(__name__, error_msg)

        # fivetran_configs = get_fivetran_configs(secret_id="fivetran-global")
        group_id = get_object_detail(
            verfiyFivetranObj, "groups", connector_vals["group_name"], fivetran_configs
        )
        if not group_id:
            error_msg = "Cannot create connector without group id"
            log_known_err_and_exit(__name__, error_msg)

        config = get_fivetran_configs(secret_id=connector_vals["configs"])
        _secret = get_fivetran_configs(secret_id="fivetran-dest-secret")
        config["secret_key"] = json.dumps(_secret)

        # log_info(__name__).info(f"Config_Obj:{config}")
        if not config:
            error_msg = "Cannot create connector without a type of defining its configs"
            log_known_err_and_exit(__name__, error_msg)

        payload = {
            "group_id": group_id,
            "service": connector_vals["service"],
            "region": self.REGION,
            "time_zone_offset": "-5",
            "run_setup_tests": self.TEST_CONNECTOR,
            "config": config,
        }
        return payload


class modify_warehouse_payload_builder(payloadBuilder):
    pass


class modify_connector_payload_builder(payloadBuilder):
    pass


class modify_destination_payload_builder(payloadBuilder):
    pass


class retrieve_warehouse_payload_builder(payloadBuilder):
    pass


class retrieve_connector_payload_builder(payloadBuilder):
    pass


class retrieve_destination_payload_builder(payloadBuilder):
    pass
