import requests
import pandas as pd
from bs4 import BeautifulSoup
url="https://ec.europa.eu/clima/ets/transaction.do?languageCode=en&startDate=&endDate=&transactionStatus=4&fromCompletionDate=&toCompletionDate=&transactionID=&transactionType=-1&suppTransactionType=-1&originatingRegistry=-1&destinationRegistry=-1&originatingAccountType=-1&destinationAccountType=-1&originatingAccountIdentifier=&destinationAccountIdentifier=&originatingAccountHolder=&destinationAccountHolder=&search=Search&currentSortSettings="

def GetFirstDateSite():
    data=requests.get(url)
    soup=BeautifulSoup(data.text,"html.parser")
    tables = soup.findChildren('table')[7]
    line = tables.findChildren(['td'])[63]
    date = line.get_text(" ",strip=True)
    return date

def GetFirstDateBdd():
    df=pd.read_csv("0-100.csv")
    return df.iloc[0,3]  
    
def refresh():
    pass # a faire
    
if __name__ == "__main__":
    if GetFirstDateBdd() != GetFirstDateSite():
        refresh()
    else:
        print("pas de nouvelles transactions")
      