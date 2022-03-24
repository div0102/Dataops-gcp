import json
from abc import ABC, abstractmethod

import requests

from base_logger import log_info
from utils import api_call_headers, autheincate


class createBaseFivetranObj(ABC):
    """Base class implements different methods used for creating and validating a fivetran object"""

    SUCCESS = "SUCCESS"
    FAIL = "FAIL"
    PASSED = "PASSED"
    CONNECTED = "CONNECTED"
    # Verify connector creation object
    SETUP_STATE = "SETUP_STATE"
    DB_CONNECTION = "CONNECTING_TO_DATABASE"
    HOST_CONNECTION = "CONNECTING_TO_HOST"
    CERT_VALIDATION = "VALIDATING_CERTIFICATE"
    VALIDATION: list = [DB_CONNECTION, HOST_CONNECTION, CERT_VALIDATION]

    @abstractmethod
    def build(self, http_protocol: str, fivetran: str, api_version: str) -> str:
        """Builds API call path"""

    @abstractmethod
    def execute(self, header, url, auth, json) -> dict:
        """Makes an actual api request for created object"""

    # @abstractmethod
    # def execute_modify(self, header, url, auth, json) -> dict:
    #     """Makes an actual api request for created object"""

    @abstractmethod
    def validate(self, response: dict) -> bool:
        """Validates the response received from acutal Api call"""

    @abstractmethod
    def store_success_resp(self, location: str) -> None:
        """Stores the result of response for further use"""


class create_warehouse(createBaseFivetranObj):

    """A group represents a warehouse within Fivetran Top of the hiearchy"""

    def build(
        self, http_protocol: str, fivetran: str, api_version: str, group_route: str
    ) -> str:

        log_info(__name__).info("Build Group API string intiliased")

        return http_protocol + "://" + fivetran + "/" + api_version + "/" + group_route

    def execute(self, header, creation_api, auth, payload) -> dict:
        log_info(__name__).info("Making actual call to API")

        try:

            response = requests.post(
                headers=header, url=creation_api, auth=auth, json=payload
            ).json()

            return response
        except Exception as e:
            log_info(__name__).exception(f"Group creation failed - {e}")

    def validate(self, response: dict):
        log_info(__name__).info("Starting the process of validating API response")
        log_info(__name__).info(f"Response received : {response}")

        if response["code"].lower() == self.SUCCESS.lower():
            return True
        else:
            code = response["code"]
            message = (
                lambda message: "Error_msg " + message
                if message
                else "Test run " + response["data"]["setup_tests"]["message"]
            )
            error_msg = message(response["message"])
            log_info(__name__).exception(
                f"Response validation failure:Group Creation Status -{code}:{error_msg}"
            )

    def store_success_resp(self, response: dict, location: str):
        log_info(__name__).info("Storing response to storage")
        try:
            with open(location, "a") as save_file:
                save_file.write(
                    json.dumps(
                        response,
                        indent=2,
                    )
                )
            return "Write Successfull"
        except Exception as e:
            log_info(__name__).exception(f"Error in writing - {e}")
            return f"Error in writing - {e}"


class create_db_connectors(createBaseFivetranObj):
    def build(
        self, http_protocol: str, fivetran: str, api_version: str, connector_route: str
    ) -> str:
        log_info(__name__).info("Build Connector API string intiliased")
        return (
            http_protocol + "://" + fivetran + "/" + api_version + "/" + connector_route
        )

    def execute(self, header, creation_api, auth, payload):
        log_info(__name__).info("Making actual call to API")
        try:
            response = requests.post(
                headers=header, url=creation_api, auth=auth, json=payload
            ).json()
            return response
        except Exception as e:
            log_info(__name__).exception(f"Db connector creation failed - {e}")

    def validate(self, response: dict):
        log_info(__name__).info("Starting the process of validating API response")
        log_info(__name__).info(f"Response received : {response}")
        val: int = 0
        total_val: int = len(self.VALIDATION)
        for validation in self.VALIDATION:
            for specific_test in response["data"]["setup_tests"]:
                test_value = str(specific_test.values())
                if validation.lower().replace("_", " ") in test_value.lower():
                    if specific_test["status"].lower() == self.PASSED.lower():
                        val += 1

        if (
            response["code"].lower() == self.SUCCESS.lower()
            and response["data"]["status"][self.SETUP_STATE.lower()]
            == self.CONNECTED.lower()
            and total_val == val
        ):
            log_out_str = f'{response["code"].lower()} : {self.SUCCESS.lower()} \
                            {response["data"]["status"][self.SETUP_STATE.lower()]} : {self.CONNECTED.lower()}'

            log_info(__name__).info(log_out_str)
            return True
        else:
            code = response["code"]
            message = (
                lambda message: "Error_msg " + message
                if message
                else "Test run " + response["data"]["setup_tests"]["message"]
            )
            error_msg = message(response["message"])
            log_info(__name__).error(
                f"Response validation failure:Connector Creation Status -{code}:{error_msg}"
            )

    def store_success_resp(self, response: dict, location: str):
        try:
            with open(location, "a") as save_file:
                save_file.write(
                    json.dumps(
                        response,
                        indent=2,
                    )
                )
            return "Write Successfull"
        except Exception as e:
            log_info(__name__).exception(f"Error in writing - {e}")
            return f"Error in writing - {e}"


class create_destination(createBaseFivetranObj):
    def build(
        self,
        http_protocol: str,
        fivetran: str,
        api_version: str,
        destination_route: str,
    ) -> str:
        return (
            http_protocol
            + "://"
            + fivetran
            + "/"
            + api_version
            + "/"
            + destination_route
        )

    def execute(self, header, creation_api, auth, payload):
        log_info(__name__).info("Making actual call to API")
        try:
            response = requests.post(
                headers=header, url=creation_api, auth=auth, json=payload
            ).json()
            return response
        except Exception as e:
            log_info(__name__).exception(f"Destination creation failed - {e}")

    def validate(self, response: dict):

        log_info(__name__).info("Starting the process of validating API response")
        log_info(__name__).info(f"Response received : {response}")

        if (
            response["code"].lower() == self.SUCCESS.lower()
            and response["data"]["setup_status"].lower() == self.CONNECTED.lower()
        ):
            if (
                response["data"]["setup_tests"][0]["status"].lower()
                == self.PASSED.lower()
            ):
                return True
        else:
            code = response["code"]
            message = (
                lambda message: "Error_msg " + message
                if message
                else "Test run " + response["data"]["setup_tests"]["message"]
            )
            error_msg = message(response["message"])
            log_info(__name__).error(
                f"Response validation failure:Destination Creation Status -{code}:{error_msg}"
            )

    def store_success_resp(self, response: dict, location: str):
        try:
            with open(location, "a") as save_file:
                save_file.write(
                    json.dumps(
                        response,
                        indent=2,
                    )
                )
            return "Write Successfull"
        except Exception as e:
            return f"Error in writing - {e}"


class fivetran:
    def __init__(
        self,
        cls: createBaseFivetranObj,
        protocol,
        core_fivetran_url,
        api_version,
        api_key,
        api_secret,
        payload,
        request_type,
        file_location,
    ) -> None:
        self.client = cls
        self.http_protocol = protocol
        self.fivetran_base_api = core_fivetran_url
        self.api_version = api_version
        self.api_key = api_key
        self.api_secret = api_secret
        self.request_payload = payload
        self.type_of_request = request_type
        self.location = file_location

    def create_fivetran_requested_object(self):

        """
        # Step -1 : Verify Payload -> responsibilty is with payload_builder class
        # Step -2 : Buil API -> build()
        # Step -3 : Authenicate and build headers -> authenicate ()
        # Step -4 : Execute API request -> execute()
        # Step -5 : Verify API response -> validate()

        """
        # self.client.verify_payload(self.request_payload)
        basic_api_call = self.client.build(
            self.http_protocol,
            self.fivetran_base_api,
            self.api_version,
            self.type_of_request,
        )
        response = self.client.execute(
            header=api_call_headers(api_key=self.api_key),
            creation_api=basic_api_call,
            auth=autheincate(self.api_key, self.api_secret),
            payload=self.request_payload,
        )
        if self.client.validate(response=response):

            print(self.client.store_success_resp(response, self.location))

            return True
        else:
            return False
