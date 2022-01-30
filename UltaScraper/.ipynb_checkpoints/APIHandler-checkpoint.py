import requests
from UltaScraper.Exceptions import HTTPError
import pandas as pd
from enum import Enum


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
            if r.status_code == 200:
                rJson = r.json()
            else:
                raise HTTPError(r.text)
            return(rJson)
    
    
