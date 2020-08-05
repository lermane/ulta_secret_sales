from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow,Flow
from google.auth.transport.requests import Request
import os
import pickle

#variables used in the following functions
SAMPLE_RANGE_NAME = 'A1:AA20000'

#copied the next 3 functions from
#https://medium.com/analytics-vidhya/how-to-read-and-write-data-to-google-spreadsheet-using-python-ebf54d51a72c

def Create_Service(client_secret_file, token_write_file, api_service_name, api_version, *scopes):
    global service
    SCOPES = [scope for scope in scopes[0]]
    
    cred = None

    if os.path.exists(token_write_file):
        with open(token_write_file, 'rb') as token:
            cred = pickle.load(token)

    if not cred or not cred.valid:
        if cred and cred.expired and cred.refresh_token:
            cred.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(client_secret_file, SCOPES)
            cred = flow.run_local_server()

        with open(token_write_file, 'wb') as token:
            pickle.dump(cred, token)

    try:
        service = build(api_service_name, api_version, credentials=cred)
        print(api_service_name, 'service created successfully')
    except Exception as e:
        print(e)

def Clear_Sheet(gsheetId):
    result_clear = service.spreadsheets().values().clear(
        spreadsheetId=gsheetId,
        range=SAMPLE_RANGE_NAME,
        body = {}
    ).execute()
    print('Sheet successfully cleared')

def Export_Data_To_Sheets(gsheetId, df):
    response_date = service.spreadsheets().values().update(
        spreadsheetId=gsheetId,
        valueInputOption='RAW',
        range=SAMPLE_RANGE_NAME,
        body=dict(
            majorDimension='ROWS',
            values=df.T.reset_index().T.values.tolist())
    ).execute()
    print('Sheet successfully updated')

#updates the sale_filter view so it will change when the numbers of rows change
def Update_Filter(gsheetId, filterId, rows, cols):
    my_range = {
    'sheetId': 0,
    'startRowIndex': 0,
    'startColumnIndex': 0,
    'endRowIndex': rows + 1,
    'endColumnIndex': cols
    }
    
    updateFilterViewRequest = {
        'updateFilterView': {
            'filter': {
                'filterViewId': filterId,
                'range': my_range
            },
            'fields': {
                'paths': 'range'
            }
        }
    }
    
    body = {'requests': [updateFilterViewRequest]}
    service.spreadsheets().batchUpdate(spreadsheetId=gsheetId, body=body).execute()
    print('Filter successfully updated')

#add hyperlinks 
def Add_Hyperlinks(sheetId, df, hyperlink_urls):
    requests = []
    
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
        requests.append(request)
        
    body = {"requests": requests}
    service.spreadsheets().batchUpdate(spreadsheetId=sheetId, body=body).execute()
    print('Hyperlinks successfully added')