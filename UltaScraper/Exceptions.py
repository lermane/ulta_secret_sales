from bs4 import BeautifulSoup


class HTTPError(Exception):
    def __init__(self, siteHTML: str):
        pageText = self.__ErrorPageParser(siteHTML)
        self.msg = "PAGE TEXT: " + pageText
        
    def __ErrorPageParser(self, siteHTML):
        soup = BeautifulSoup(siteHTML, 'html.parser')
        return(soup.body.text)
    

class NoDataError(Exception):
    def __init__(self, idValue: str):
        self.msg = "No data available"
        
        
class MessagesExistError(Exception):
    def __init__(self, messagesDict: dict):
        strMsg = "Messages exist"
        
        if 'items' in messagesDict:
            for item in messagesDict['items']:
                 strMsg = strMsg + '\nTYPE: ' + item['type'] + '\tMESSAGE: ' + item['message']
                    
        self.msg = strMsg
        