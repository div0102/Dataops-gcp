# -*- coding: utf-8 -*-
"""
Created on Tue Mar 15 11:41:41 2022

@author: shahris
"""


import json

import requests
from requests.auth import HTTPBasicAuth

api_key = "kIVnsK2i9paDLij2"
api_secret = "11S8JSYAfkIHnqBAphbaixBhw6mxpsUY"


auth = HTTPBasicAuth(api_key, api_secret)

data = {}

headers = {"Authorization": "Basic " + api_key, "Content-Type": "application/json"}

# declare connector_id here
connector_id = "periosteum_uncoiled"

# declare url here
# url = "https://api.fivetran.com/v1/connectors/{}".format(connector_id)
url = "https://api.fivetran.com/v1/destinations/fishing_paramedic"


# delete connector
response = requests.delete(url=url, auth=auth).json()
print(response)
