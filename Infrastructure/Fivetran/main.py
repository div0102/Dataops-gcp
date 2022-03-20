from base_logger import log_info
from create_base import (
    create_connectors,
    create_destination,
    create_warehouse,
    fivetran,
)
from payload_builder_base import (
    create_connector_payload_builder,
    create_destination_payload_builder,
    create_warehouse_payload_builder,
)
from utils import get_fivetran_configs

file_location = (
    "C:\\Learning\\Dataops-gcp\\Infrastructure\\Fivetran\\data\\connector_info.txt"
)


def get_request_type(
    type_of_request, group_name: str, service_name=["", ""], configs_name=["", ""]
):

    request_type = {
        "all": {
            "groups": [
                create_warehouse,
                create_warehouse_payload_builder,
                {"name": group_name},
            ],
            "connectors": [
                create_connectors,
                create_connector_payload_builder,
                {
                    "service": service_name[0],
                    "group_name": group_name,
                    "configs": configs_name[0],
                },
            ],
            "destinations": [
                create_destination,
                create_destination_payload_builder,
                {
                    "service": service_name[1],
                    "group_name": group_name,
                    "configs": configs_name[1],
                },
            ],
        },
        "groups": [
            create_warehouse,
            create_warehouse_payload_builder,
            {"name": group_name},
        ],
        "connectors": [
            create_connectors,
            create_connector_payload_builder,
            {
                "service": service_name[0],
                "group_name": group_name,
                "configs": configs_name[0],
            },
        ],
        "destinations": [
            create_destination,
            create_destination_payload_builder,
            {
                "service": service_name[0],
                "group_name": group_name,
                "configs": configs_name[0],
            },
        ],
    }

    return type_of_request, request_type[type_of_request]


def obj_creation(cls_obj, payload, obj_type, storage_location, fivetran_configs):
    create_fivetran_obj = fivetran(
        cls_obj,
        fivetran_configs["protocol"],
        fivetran_configs["core_fivetran_url"],
        fivetran_configs["api_version"],
        fivetran_configs["api_key"],
        fivetran_configs["api_secret"],
        payload,
        obj_type,
        storage_location,
    )

    if create_fivetran_obj.create_fivetran_requested_object():
        print(f"{obj_type} creation successfull")
    else:
        print(f"Error in {obj_type} creation check logs")
        exit


def build_all(obj_type, obj_cls_map, storage_location):
    fivetran_configs = get_fivetran_configs(secret_id="fivetran-global")

    if obj_type.lower() == "all":
        """This requests creates groups -> connectors -> destination sequentially"""
        for obj_type, obj_cls in obj_cls_map.items():

            payload = obj_cls[1]().build_payload(obj_cls[2])
            req_obj = obj_cls[0]()
            obj_creation(req_obj, payload, obj_type, storage_location, fivetran_configs)

    else:
        payload = obj_cls_map[1]().build_payload(obj_cls_map[2])
        req_obj = obj_cls_map[0]()
        obj_creation(req_obj, payload, obj_type, storage_location, fivetran_configs)


def main(create_obj_of_type, create_obj_cls_params, storage_location):
    log_info(__name__).info("Build started for creating Fivetran objects")
    log_info(__name__).info(
        f"Object-type : {create_obj_of_type} Object-params : {create_obj_cls_params}"
    )
    build_all(create_obj_of_type, create_obj_cls_params, storage_location)


if __name__ == "__main__":

    log_info(__name__).info("Fivetran module execution started")

    """Below Parms All creates a warehouse connector information passed and desitination"""

    # create_obj_of_type, create_obj_cls_params = get_request_type(
    #     "all",
    #     group_name="warehouse_11",
    #     service_name=["google_cloud_sqlserver", "big_query"],
    #     configs_name=["fivetran-src-conn", "fivetran-dest-conn"],
    # )
    """Below Parms groups creates groups as per the information passed"""

    # create_obj_of_type, create_obj_cls_params = get_request_type(
    #     "groups",
    #     group_name="warehouse_11",
    # )
    """Below Parms connectors creates connector in the specified warehouse as per the information passed"""
    # create_obj_of_type, create_obj_cls_params = get_request_type(
    #     "connectors",
    #     group_name="warehouse_11",
    #     service_name=["google_cloud_sqlserver", ""],
    #     configs_name=["fivetran-src-conn", ""],
    # )
    """Below Parms connectors creates destination in the specified warehouse as per the information passed"""
    create_obj_of_type, create_obj_cls_params = get_request_type(
        "destinations",
        group_name="warehouse_11",
        service_name=["big_query", ""],
        configs_name=["fivetran-dest-conn", ""],
    )
    # print(create_obj_of_type, create_obj_cls_params)
    main(create_obj_of_type, create_obj_cls_params, file_location)
