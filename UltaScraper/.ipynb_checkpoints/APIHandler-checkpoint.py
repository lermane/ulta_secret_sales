import requests
from UltaScraper.Exceptions import HTTPError
from UltaScraper.Exceptions import NoDataError
from UltaScraper.Exceptions import MessagesExistError
import pandas as pd
from enum import Enum
import json


def is_json(myjson):
    try:
        json.loads(myjson)
    except ValueError as e:
        return False
    return True


class APIEndpoint(Enum):
    PRODUCT = "https://www.ulta.com/services/v5/catalog/product/"
    SKU = "https://www.ulta.com/services/v5/catalog/sku/"
    DYNAMIC = "https://www.ulta.com/services/v5/pdp/dynamicdata?skuId="

class APIHandler:
    _idTypeBaseUrl: APIEndpoint
    _idValue: str
    url: str

    @property
    def url(self):
        return self.url
        
    def __init__(self, idType: APIEndpoint, idValue: str):
        self._idTypeBaseUrl = idType.value
        self._idValue = idValue
        self._url = self._idTypeBaseUrl + self._idValue
        
    def get(self):
        with requests.get(self._url) as r:
            if r.status_code == 200 and is_json(r.text):
                rJson = r.json()
            else:
                raise HTTPError(r.text)
             
            if 'data' not in rJson:
                raise NoDataError(self._idValue)
                
            if 'messages' in rJson['data']:
                raise MessagesExistError(rJson['data']['messages'])
            
            return(rJson)
    
    
