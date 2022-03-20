from abc import ABC, abstractmethod


class deleteBaseFivetranObj(ABC):
    @abstractmethod
    def payload(self):
        pass

    def build(self):
        pass

    def execute(self):
        pass

    def validate(self):
        pass


class delete_warehouse(deleteBaseFivetranObj):
    """
    https://api.fivetran.com/v1/groups/{group_id}

    """

    def payload(self, group_name):
        pass

    def build(
        self, http_protocol: str, fivetran: str, api_version: str, group_route: str
    ) -> str:
        return http_protocol + "://" + fivetran + "/" + api_version + "/" + group_route


class delete_connectors(deleteBaseFivetranObj):
    pass


class delete_destination(deleteBaseFivetranObj):
    pass
