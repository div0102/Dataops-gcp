import requests

from utils import api_call_headers, autheincate


class verfiyFivetranObj:
    def __init__(
        self, get_details_for, get_details_for_object, fivetran_configs: dict
    ) -> None:
        self.detail_type = get_details_for
        self.object_name = get_details_for_object
        self.http_protocol = fivetran_configs["protocol"]
        self.fivetran = fivetran_configs["core_fivetran_url"]
        self.api_version = fivetran_configs["api_version"]
        self.api_key = fivetran_configs["api_key"]
        self.api_secret = fivetran_configs["api_secret"]
        self.cursor_limit = str(100)

    def build(self, cursor) -> str:
        """Generic method to build API call use
        detail_type field to pass information for which you need to retrieve information
        """
        print(
            self.http_protocol,
            self.fivetran,
            self.api_version,
            self.detail_type,
            self.cursor_limit,
        )
        return (
            self.http_protocol
            + "://"
            + self.fivetran
            + "/"
            + self.api_version
            + "/"
            + self.detail_type
            + "?curosr="
            + cursor
            + "?"
            + self.cursor_limit
        )

    def excute(self, request):
        response = requests.get(
            headers=api_call_headers(self.api_key),
            url=request,
            auth=autheincate(self.api_key, self.api_secret),
        ).json()
        return response

    def search_object_by_name(self, response, object_type):
        """Pass the object name to be searched in the response
        Difference object has different response type i.e
        Groups has reponse from connectors.
        """
        print(response)
        if response["code"].lower() == "Success".lower():
            for requested_data in response["data"]["items"]:

                for key, value in requested_data.items():

                    if (
                        object_type.lower() == "groups"
                        and key.lower() == "name"
                        and value.lower() == self.object_name
                    ):

                        return ("Data_Found", requested_data["id"])
                    elif (
                        object_type.lower() == "connectors"
                        and key.lower() == "schema"
                        and value.lower() == self.object_name
                    ):

                        return ("Data_Found", requested_data["id"])
            else:
                try:
                    return ("Next_Cursor", response["data"]["next_cursor"])
                except:
                    return ("List_Exhausted", "No new records to fetch")
