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

    @abstractmethod
    def build(self, http_protocol: str, fivetran: str, api_version: str) -> str:
        """Builds API call path"""

    @abstractmethod
    def verify_payload(self, payload: dict) -> None:
        """Validates request payload"""

    @abstractmethod
    def execute(self, header, url, auth, json) -> dict:
        """Makes an actual api request for created object"""

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

    def verify_payload(self, payload: dict):
        """Sanity check of payload though payload is being verified and constructured in payload_builder"""
        log_info(__name__).info("Verify payload passed to API")
        if isinstance(payload, dict):
            key_name = list(payload.keys())[0]
            group_name = list(payload.values())[0]

            if not group_name or key_name != "name":
                raise ValueError(
                    f"Either group_name is null or payload key is not name"
                )
        else:
            raise Exception(f"payload object is not a dict please pass is as a dict")

    def execute(self, header, creation_api, auth, payload) -> dict:
        log_info(__name__).info("Making actual call to API")

        response = requests.post(
            headers=header, url=creation_api, auth=auth, json=payload
        ).json()

        return response

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
                f"Response validation failure:Group Creation Status{code} {error_msg}"
            )
            # raise Exception(
            #     f"Response validation failure:Group Creation Status{code} {error_msg}"
            # )

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


class create_connectors(createBaseFivetranObj):
    def build(
        self, http_protocol: str, fivetran: str, api_version: str, connector_route: str
    ) -> str:
        log_info(__name__).info("Build Connector API string intiliased")
        return (
            http_protocol + "://" + fivetran + "/" + api_version + "/" + connector_route
        )

    def execute(self, header, creation_api, auth, payload):
        log_info(__name__).info("Making actual call to API")
        response = requests.post(
            headers=header, url=creation_api, auth=auth, json=payload
        ).json()

        return response

    def verify_payload(self, payload: dict):
        """Sanity check of payload though payload is being verified and constructured in payload_builder"""

        if isinstance(payload, dict):
            group_name = list(payload.values())[0]
            if not group_name:
                log_info(__name__).exception(f"group_name cannot be null")
        else:
            log_info(__name__).exception(
                f"payload object is not a dict please pass is as a dict"
            )

    def validate(self, response: dict):
        log_info(__name__).info("Starting the process of validating API response")
        log_info(__name__).info(f"Response received : {response}")

        if (
            response["code"].lower() == self.SUCCESS.lower()
            and response["data"]["setup_tests"][0]["status"].lower()
            == self.PASSED.lower()
            and response["data"]["setup_tests"][1]["status"].lower()
            == self.PASSED.lower()
            and response["data"]["setup_tests"][2]["status"].lower()
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
            log_info(__name__).exception(
                f"Response validation failure:Group Creation Status{code} {error_msg}"
            )
            # raise Exception(
            #     f"Response validation failure:Connector Creation Status{code} {error_msg}"
            # )

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
        response = requests.post(
            headers=header, url=creation_api, auth=auth, json=payload
        ).json()
        return response

    def verify_payload(self, payload: dict):
        """Sanity check of payload though payload is being verified and constructured in payload_builder"""
        log_info(__name__).info("I am verify destination payload")

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
            log_info(__name__).exception(
                f"Response validation failure:Group Creation Status{code} {error_msg}"
            )
            # raise Exception(
            #     f"Response validation failure:Destination Creation Status{code} {error_msg}"
            # )

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
        c: createBaseFivetranObj,
        protocol,
        core_fivetran_url,
        api_version,
        api_key,
        api_secret,
        payload,
        request_type,
        file_location,
    ) -> None:
        self.client = c
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
        # Step -1 : Verify Payload -> verify_payload()
        # Step -2 : Buil API -> build()
        # Step -3 : Authenicate and build headers -> authenicate ()
        # Step -4 : Execute API request -> execute()
        # Step -5 : Verify API response -> validate()

        """
        self.client.verify_payload(self.request_payload)
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
