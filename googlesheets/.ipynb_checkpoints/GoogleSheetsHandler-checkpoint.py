from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow,Flow
from google.auth.transport.requests import Request
import os
import pickle
from configparser import ConfigParser


#variables used in the following functions
SAMPLE_RANGE_NAME = 'A1:AA20000'

def config(filename='/home/lermane/Documents/ulta_secret_sales/googlesheets/googlesheets.ini', section='dev'):
    """ this function parses the googlesheets.ini file and returns it in dictionary form """
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    gs = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            gs[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return gs


class GoogleSheetsHandler:
    _environment: str
    _params: dict
        
    def __init__(self, environment: str):
        self._environment = environment
        self._params = config(section=self._environment)
        self._service = None
        
        
    def __enter__(self):
        # This ensure, whenever an object is created using "with"
        # this magic method is called, where you can create the connection.
        self._create_service()
        return self

    
    def __exit__(self, exception_type, exception_val, trace):
        pass
    
        
    def _create_service(self, apiServiceName = 'sheets', apiVersion = 'v4', scopes = ['https://www.googleapis.com/auth/spreadsheets']):
        """ creates connection to google sheets using the google sheets api """
        SCOPES = [scope for scope in scopes[0]]

        cred = None

        if os.path.exists(self._params['pickle']):
            with open(self._params['pickle'], 'rb') as token:
                cred = pickle.load(token)

        if not cred or not cred.valid:
            if cred and cred.expired and cred.refresh_token:
                cred.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(self._params['credentials'], SCOPES)
                cred = flow.run_local_server()

            with open(self._params['pickle'], 'wb') as token:
                pickle.dump(cred, token)

        try:
            self._service = build(apiServiceName, apiVersion, credentials=cred)
            print(apiServiceName, 'service created successfully')
        except Exception as e:
            print(e)
        

    def clear_sheet(self):
        resultClear = self._service.spreadsheets().values().clear(
            spreadsheetId=self._params['sheetid'],
            range=SAMPLE_RANGE_NAME,
            body = {}
        ).execute()
        print('Sheet successfully cleared')

    def export_dataframe_to_sheets(self, df):
        response_date = self._service.spreadsheets().values().update(
            spreadsheetId=self._params['sheetid'],
            valueInputOption='RAW',
            range=SAMPLE_RANGE_NAME,
            body=dict(
                majorDimension='ROWS',
                values=df.T.reset_index().T.values.tolist())
        ).execute()
        print('Sheet successfully updated')

        
    def update_filter(self, rows, cols):
        """ updates the sale_filter view so it will change when the numbers of rows change """
        _range = {
        'sheetId': 0,
        'startRowIndex': 0,
        'startColumnIndex': 0,
        'endRowIndex': rows + 1,
        'endColumnIndex': cols
        }

        updateFilterViewRequest = {
            'updateFilterView': {
                'filter': {
                    'filterViewId': self._params['filterid'],
                    'range': _range
                },
                'fields': {
                    'paths': 'range'
                }
            }
        }
        body = {'requests': [updateFilterViewRequest]}
        self._service.spreadsheets().batchUpdate(spreadsheetId=self._params['sheetid'], body=body).execute()
        print('Filter successfully updated')
              
    
    def add_header(self):
        """ creates a header for the dataframe that is black and static """
        _range = {
        'sheetId': 0,
        'startRowIndex': 0,
        'endRowIndex': 1
        }

        addHeaderRequests = [{
          "repeatCell": {
            "range": _range,
            "cell": {
              "userEnteredFormat": {
                "backgroundColor": {
                  "red": 0.0,
                  "green": 0.0,
                  "blue": 0.0
                },
                "horizontalAlignment" : "CENTER",
                "textFormat": {
                  "foregroundColor": {
                    "red": 1.0,
                    "green": 1.0,
                    "blue": 1.0
                  },
                  "fontSize": 12,
                  "bold": True
                }
              }
            },
            "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"
          }
        },
        {
          "updateSheetProperties": {
            "properties": {
              "sheetId": 0,
              "gridProperties": {
                "frozenRowCount": 1
              }
            },
            "fields": "gridProperties.frozenRowCount"
          }
        }]

        body = {'requests': addHeaderRequests}
        self._service.spreadsheets().batchUpdate(spreadsheetId=self._params['sheetid'], body=body).execute()
        print('header successfully added')

        
    def resize_rows(self, rows):
        autoResizeRowsRequest = {
            "autoResizeDimensions" : {
                "dimensions" : {
                "sheetId": 0,
                "dimension": "ROWS",
                "startIndex": 0,
                "endIndex" : rows + 1
                }
            }
        }
        body = {'requests': [autoResizeRowsRequest]}
        self._service.spreadsheets().batchUpdate(spreadsheetId=self._params['sheetid'], body=body).execute()
        print('Rows successfully updated')
        

    def add_hyperlinks(self, df, hyperlinkUrls):
        addHyperlinksRequest = []

        for i in range(len(df)):
            hyperlink = '"' + hyperlinkUrls[i] + '"'
            hypertext = '"' + df.iloc[i]['product_name'] + '"'
            request = {
                "updateCells": {
                    "rows": [
                        {
                            "values": [{
                                "userEnteredValue": {
                                    "formulaValue": "=HYPERLINK({link}, {text})".format(link = hyperlink, text = hypertext)
                                }
                            }]
                        }
                    ],
                    "fields": "userEnteredValue",
                    "start": {
                        "sheetId": 0,
                        "rowIndex": i + 1,
                        "columnIndex": 4
                    }
                }
            }
            addHyperlinksRequest.append(request)
        body = {'requests': addHyperlinksRequest}
        self._service.spreadsheets().batchUpdate(spreadsheetId=self._params['sheetid'], body=body).execute()
        print('Hyperlinks successfully added')


    def add_percent_format(self, rows):
        """ changes percent_off number format in the google docs to a percent """
        my_range = {
        'sheetId': 0,
        'startRowIndex': 1,
        'startColumnIndex': 7,
        'endRowIndex': rows + 1,
        'endColumnIndex': 8
        }

        addPercentFormatRequest = {
          "repeatCell": {
            "range": my_range,
            "cell": {
              "userEnteredFormat": {
                "numberFormat": {
                  "type": "NUMBER",
                  "pattern": "00.00%"
                }
              }
            },
            "fields": "userEnteredFormat.numberFormat"
          }
        }
        body = {'requests': [addPercentFormatRequest]}
        self._service.spreadsheets().batchUpdate(spreadsheetId=self._params['sheetid'], body=body).execute()
        print('percent_off number format successfully changed')


    def add_currency_format(self, rows):
        """ changes current_price and max_price format in the google docs to currency """
        currentPriceFormatRequest = {
          "repeatCell": {
            "range": {
                'sheetId': 0,
                'startRowIndex': 1,
                'startColumnIndex': 5,
                'endRowIndex': rows + 1,
                'endColumnIndex': 6
            },
            "cell": {
              "userEnteredFormat": {
                "numberFormat": {
                  "type": "CURRENCY",
                  "pattern": "$0.00"
                }
              }
            },
            "fields": "userEnteredFormat.numberFormat"
          }
        }

        maxPriceFormatRequest = {
          "repeatCell": {
            "range": {
                'sheetId': 0,
                'startRowIndex': 1,
                'startColumnIndex': 6,
                'endRowIndex': rows + 1,
                'endColumnIndex': 7
             },
            "cell": {
              "userEnteredFormat": {
                "numberFormat": {
                  "type": "CURRENCY",
                  "pattern": "$0.00"
                }
              }
            },
            "fields": "userEnteredFormat.numberFormat"
          }
        }

        body = {'requests': [currentPriceFormatRequest, maxPriceFormatRequest]}
        self._service.spreadsheets().batchUpdate(spreadsheetId=self._params['sheetid'], body=body).execute()
        print('currency format successfully changed')
        
        
    def resize_columns(self, rows):
        requests = []
        columnInfo = {
            'category': {'start': 0, 'end': 1, 'pixelSize': 150},
            'sub_category': {'start': 1, 'end': 2, 'pixelSize': 150},
            'sub_sub_category': {'start': 2, 'end': 3, 'pixelSize': 185},
            'brand': {'start': 3, 'end': 4, 'pixelSize': 200},
            'name': {'start': 4, 'end': 5, 'pixelSize': 300},
            'current_price': {'start': 5, 'end': 6, 'pixelSize': 140},
            'max_price': {'start': 6, 'end': 7, 'pixelSize': 125},
            'percent_off': {'start': 7, 'end': 8, 'pixelSize': 125},
            'variants': {'start': 8, 'end': 9, 'pixelSize': 600},
            'offers': {'start': 9, 'end': 10, 'pixelSize': 200}
        }

        #name column
        for key, value in columnInfo.items():
            requests.append(
                {
                    "updateDimensionProperties" : {
                        "range" : {
                            "sheetId" : 0,
                            "dimension" : "COLUMNS",
                            "startIndex" : columnInfo[key]['start'],
                            "endIndex" : columnInfo[key]['end']
                        },
                        "properties" : {
                            "pixelSize": columnInfo[key]['pixelSize']
                        },
                        "fields": "pixelSize"
                    }
                }
            ) 
            if key in ['name', 'brand', 'variants', 'offers']:
                requests.append(
                    {
                      "repeatCell": {
                        "range": {
                            'sheetId': 0,
                            'startRowIndex': 1,
                            'startColumnIndex': columnInfo[key]['start'],
                            'endRowIndex': rows + 1,
                            'endColumnIndex': columnInfo[key]['end']
                            },
                        "cell": {
                          "userEnteredFormat": {
                            "wrapStrategy": "WRAP"
                          }
                        },
                        "fields": "userEnteredFormat.wrapStrategy"
                      }
                    }
                )

        body = {'requests': requests}

        self._service.spreadsheets().batchUpdate(spreadsheetId=self._params['sheetid'], body=body).execute()
        print('Columns successfully updated')