from abc import ABC, abstractmethod

from base_logger import log_info
from utils import get_fivetran_configs, get_object_detail
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
    def build_payload(self, warehouse_name: dict) -> dict:
        log_info(__name__).info(f"Buildig payload for group request")
        if not warehouse_name["name"]:
            log_info(__name__).exception(f"Cannot create a warehouse without a name")
        payload = {"name": warehouse_name["name"]}
        return payload


class create_connector_payload_builder(payloadBuilder):
    def build_payload(self, connector_vals: dict) -> dict:

        if not connector_vals["service"]:
            log_info(__name__).exception(
                f"Cannot create connector without a type of service"
            )
        fivetran_configs = get_fivetran_configs(secret_id="fivetran-global")
        group_id = get_object_detail(
            verfiyFivetranObj, "groups", connector_vals["group_name"], fivetran_configs
        )
        if not group_id:
            log_info(__name__).exception(f"Cannot create connector without group id")
        config = get_fivetran_configs(secret_id=connector_vals["configs"])
        if not config:
            log_info(__name__).exception(
                f"Cannot create connector without a type of defining its configs"
            )
        log_info(__name__).info(f"Buildig payload for connector request")

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
    def build_payload(self, connector_vals: dict) -> dict:

        if not connector_vals["service"]:
            log_info(__name__).exception(
                f"Cannot create connector without a type of service"
            )
        fivetran_configs = get_fivetran_configs(secret_id="fivetran-global")
        group_id = get_object_detail(
            verfiyFivetranObj, "groups", connector_vals["group_name"], fivetran_configs
        )
        if not group_id:
            log_info(__name__).exception(f"Cannot create connector without group id")
        config = get_fivetran_configs(secret_id=connector_vals["configs"])
        if not config:
            log_info(__name__).exception(
                f"Cannot create connector without a type of defining its configs"
            )
        log_info(__name__).info(f"Buildig payload for destination request")

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
