from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow,Flow
from google.auth.transport.requests import Request
import os
import pickle
import google_sheets_id

#variables used in the following functions
gsheetId = google_sheets_id.get_sheet_id()
SAMPLE_RANGE_NAME = 'A1:AA20000'

#copied the next 3 functions from
#https://medium.com/analytics-vidhya/how-to-read-and-write-data-to-google-spreadsheet-using-python-ebf54d51a72c

def Create_Service(client_secret_file, api_service_name, api_version, *scopes):
    global service
    SCOPES = [scope for scope in scopes[0]]
    
    cred = None

    if os.path.exists('token_write.pickle'):
        with open('token_write.pickle', 'rb') as token:
            cred = pickle.load(token)

    if not cred or not cred.valid:
        if cred and cred.expired and cred.refresh_token:
            cred.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(client_secret_file, SCOPES)
            cred = flow.run_local_server()

        with open('token_write.pickle', 'wb') as token:
            pickle.dump(cred, token)

    try:
        service = build(api_service_name, api_version, credentials=cred)
        print(api_service_name, 'service created successfully')
        #return service
    except Exception as e:
        print(e)
        #return None

def Clear_Sheet():
    result_clear = service.spreadsheets().values().clear(
        spreadsheetId=gsheetId,
        range=SAMPLE_RANGE_NAME,
        body = {}
    ).execute()
    print('Sheet successfully cleared')

def Export_Data_To_Sheets(df):
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
def Update_Filter(i):
    my_range = {
    'sheetId': 0,
    'startRowIndex': 0,
    'startColumnIndex': 0,
    'endRowIndex': i + 1,
    'endColumnIndex': 11
    }
    
    updateFilterViewRequest = {
        'updateFilterView': {
            'filter': {
                'filterViewId': '2092242562',
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
