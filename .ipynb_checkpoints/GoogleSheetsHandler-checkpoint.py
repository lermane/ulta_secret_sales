from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow,Flow
from google.auth.transport.requests import Request
import os
import pickle
from configparser import ConfigParser


#variables used in the following functions
SAMPLE_RANGE_NAME = 'A1:AA20000'

def config(filename='/home/lermane/Documents/ulta_secret_sales/googlesheets.ini', section='dev'):
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

        
    #updates the sale_filter view so it will change when the numbers of rows change
    def update_filter(self, rows, cols):
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

    
    #add hyperlinks 
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
                        "columnIndex": 0
                    }
                }
            }
            addHyperlinksRequest.append(request)
        body = {'requests': addHyperlinksRequest}
        self._service.spreadsheets().batchUpdate(spreadsheetId=self._params['sheetid'], body=body).execute()
        print('Hyperlinks successfully added')

        
    #change percent_off number format in the google docs to a percent
    def add_percent_format(self, rows):
        _range = {
        'sheetId': 0,
        'startRowIndex': 1,
        'startColumnIndex': 4,
        'endRowIndex': rows + 1,
        'endColumnIndex': 5
        }

        addPercentFormatRequest = {
          "repeatCell": {
            "range": _range,
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

        
    #add the condition where, if the age column value is "new", color it orange. I only have to do this once ever.
    def add_conditional_format(self):
        _range = {
            'sheetId': 0,
            'startRowIndex': 0,
            'startColumnIndex': 0,
            'endRowIndex': 160,
            'endColumnIndex': 14
            }

        addConditionalFormatRequest = {
              "addConditionalFormatRule": {
                "rule": {
                  "ranges" : [_range],
                  "booleanRule": {
                    "condition": {
                      "type": "CUSTOM_FORMULA",
                      "values": [
                        {
                          "userEnteredValue": '={sheet}!$N1="new"'.format(sheet = self._params['sheetname'])
                        }
                      ]
                    },
                    "format": {
                      "backgroundColor": {
                        "red" : 0.9764705882352941,
                        "green" : 0.796078431372549,
                        "blue" : 0.611764705882353
                      }
                    }
                  }
                },
                "index": 0
              }
            }
        body = {'requests': [addConditionalFormatRequest]}
        self._service.spreadsheets().batchUpdate(spreadsheetId=self._params['sheetid'], body=body).execute()
        print('conditional format successfully changed')
    
    
    def resize_columns(self, rows):
        #name column
        resizeNameColumnRequest = {
            "updateDimensionProperties" : {
                "range" : {
                    "sheetId" : 0,
                    "dimension" : "COLUMNS",
                    "startIndex" : 0,
                    "endIndex" : 1
                },
                "properties" : {
                    "pixelSize": 350
                },
                "fields": "pixelSize"
            }
        }    
        wrapNameColumnRequest = {
          "repeatCell": {
            "range": {
                'sheetId': 0,
                'startRowIndex': 1,
                'startColumnIndex': 0,
                'endRowIndex': rows + 1,
                'endColumnIndex': 1
                },
            "cell": {
              "userEnteredFormat": {
                "wrapStrategy": "WRAP"
              }
            },
            "fields": "userEnteredFormat.wrapStrategy"
          }
        }

        #product column
        resizeBrandColumnRequest = {
            "updateDimensionProperties" : {
                "range" : {
                    "sheetId" : 0,
                    "dimension" : "COLUMNS",
                    "startIndex" : 1,
                    "endIndex" : 2
                },
                "properties" : {
                    "pixelSize": 200
                },
                "fields": "pixelSize"
            }
        }    
        wrapBrandColumnRequest = {
          "repeatCell": {
            "range": {
                'sheetId': 0,
                'startRowIndex': 1,
                'startColumnIndex': 1,
                'endRowIndex': rows + 1,
                'endColumnIndex': 2
                },
            "cell": {
              "userEnteredFormat": {
                "wrapStrategy": "WRAP"
              }
            },
            "fields": "userEnteredFormat.wrapStrategy"
          }
        }

        #options column
        resizeVariantsColumnRequest = {
            "updateDimensionProperties" : {
                "range" : {
                    "sheetId" : 0,
                    "dimension" : "COLUMNS",
                    "startIndex" : 5,
                    "endIndex" : 6
                },
                "properties" : {
                    "pixelSize": 600
                },
                "fields": "pixelSize"
            }
        }    
        wrapVariantsColumnRequest = {
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
                "wrapStrategy": "WRAP"
              }
            },
            "fields": "userEnteredFormat.wrapStrategy"
          }
        }

        autoResizeColumnsRequest = {
            "autoResizeDimensions" : {
                "dimensions" : {
                "sheetId": 0,
                "dimension": "COLUMNS",
                "startIndex": 0,
                "endIndex" : 20
                }
            }
        }

        body = {
            'requests': [
                autoResizeColumnsRequest, 
                wrapNameColumnRequest, 
                resizeNameColumnRequest, 
                wrapBrandColumnRequest, 
                resizeBrandColumnRequest,
                wrapVariantsColumnRequest, 
                resizeVariantsColumnRequest
            ]
        }

        self._service.spreadsheets().batchUpdate(spreadsheetId=self._params['sheetid'], body=body).execute()
        print('Columns successfully updated')
        
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
        

    def Resize_Columns(self, rows):
        #name column
        resizeNameColumnRequest = {
            "updateDimensionProperties" : {
                "range" : {
                    "sheetId" : 0,
                    "dimension" : "COLUMNS",
                    "startIndex" : 3,
                    "endIndex" : 4
                },
                "properties" : {
                    "pixelSize": 323
                },
                "fields": "pixelSize"
            }
        }    
        wrapNameColumnRequest = {
          "repeatCell": {
            "range": {
                'sheetId': 0,
                'startRowIndex': 1,
                'startColumnIndex': 3,
                'endRowIndex': rows + 1,
                'endColumnIndex': 4
                },
            "cell": {
              "userEnteredFormat": {
                "wrapStrategy": "WRAP"
              }
            },
            "fields": "userEnteredFormat.wrapStrategy"
          }
        }

        #product column
        resizeProductColumnRequest = {
            "updateDimensionProperties" : {
                "range" : {
                    "sheetId" : 0,
                    "dimension" : "COLUMNS",
                    "startIndex" : 5,
                    "endIndex" : 6
                },
                "properties" : {
                    "pixelSize": 289
                },
                "fields": "pixelSize"
            }
        }    
        wrapProductColumnRequest = {
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
                "wrapStrategy": "WRAP"
              }
            },
            "fields": "userEnteredFormat.wrapStrategy"
          }
        }

        #options column
        resizeOptionsColumnRequest = {
            "updateDimensionProperties" : {
                "range" : {
                    "sheetId" : 0,
                    "dimension" : "COLUMNS",
                    "startIndex" : 9,
                    "endIndex" : 10
                },
                "properties" : {
                    "pixelSize": 246
                },
                "fields": "pixelSize"
            }
        }    
        wrapOptionsColumnRequest = {
          "repeatCell": {
            "range": {
                'sheetId': 0,
                'startRowIndex': 1,
                'startColumnIndex': 9,
                'endRowIndex': rows + 1,
                'endColumnIndex': 10
                },
            "cell": {
              "userEnteredFormat": {
                "wrapStrategy": "WRAP"
              }
            },
            "fields": "userEnteredFormat.wrapStrategy"
          }
        }

        autoResizeColumnsRequest = {
            "autoResizeDimensions" : {
                "dimensions" : {
                "sheetId": 0,
                "dimension": "COLUMNS",
                "startIndex": 0,
                "endIndex" : 20
                }
            }
        }

        body = {
            'requests': [
                autoResizeColumnsRequest, 
                wrapNameColumnRequest, 
                resizeNameColumnRequest, 
                wrapProductColumnRequest, 
                resizeProductColumnRequest,
                wrapOptionsColumnRequest, 
                resizeOptionsColumnRequest
            ]
        }

        self._service.spreadsheets().batchUpdate(spreadsheetId=self._params['sheetid'], body=body).execute()
        print('Columns successfully updated')

    
    #add hyperlinks 
    def Add_Hyperlinks(self, df, hyperlink_urls):
        addHyperlinksRequest = []
        for i in range(len(df)):
            hyperlink = '"' + hyperlink_urls[i] + '"'
            hypertext = '"' + df.iloc[i]['name'] + '"'
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
                        "columnIndex": 3
                    }
                }
            }
            addHyperlinksRequest.append(request)
        body = {'requests': addHyperlinksRequest}
        self._service.spreadsheets().batchUpdate(spreadsheetId=self._params['sheetid'], body=body).execute()
        print('Hyperlinks successfully added')

        
    #change percent_off number format in the google docs to a percent
    def Add_Percent_Format(self, rows):
        my_range = {
        'sheetId': 0,
        'startRowIndex': 1,
        'startColumnIndex': 8,
        'endRowIndex': rows + 1,
        'endColumnIndex': 9
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