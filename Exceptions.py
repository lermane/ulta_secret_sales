from bs4 import BeautifulSoup

class HTTPError(Exception):
    def __init__(self, siteHTML: str):
        pageText = self.__ErrorPageParser(siteHTML)
        self.msg = "PAGE TEXT: " + pageText
        
    def __ErrorPageParser(self, siteHTML):
        soup = BeautifulSoup(siteHTML, 'html.parser')
        return(soup.body.text)
        