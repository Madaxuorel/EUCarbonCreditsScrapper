from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import NoSuchElementException 
from selenium.common.exceptions import ElementNotInteractableException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException

import pandas as pd
from bs4 import BeautifulSoup
from time import *
#61:76
#composition lignes : 0-15 infos + 2 details
class scrapper:
    def __init__(self):
        self.starttime = time()
        self.driver = webdriver.Chrome(executable_path="chromedriver")
        self.driver.get("https://ec.europa.eu/clima/ets/transaction.do?languageCode=fr&startDate=&endDate=&transactionStatus=4&fromCompletionDate=&toCompletionDate=&transactionID=&transactionType=-1&suppTransactionType=-1&originatingRegistry=-1&destinationRegistry=-1&originatingAccountType=-1&destinationAccountType=-1&originatingAccountIdentifier=&destinationAccountIdentifier=&originatingAccountHolder=&destinationAccountHolder=&search=Search&currentSortSettings=")
        self.counterlinepage = 0
        self.counterpage = 0
        self.dictlist = []
        self.getsoup()
        
    def getsoup(self):

        while self.counterpage < 100:
            print("Analyse de la page {}".format(self.counterpage+1))
            soup = BeautifulSoup(self.driver.page_source,"lxml")
            tables = soup.findChildren('table')[7]
            self.lines = tables.findChildren(['td'])
            while self.counterlinepage < 20:
                
                self.getsoupdata()
                self.counterlinepage += 1
            self.getnextpage()
            self.counterlinepage = 0
            self.counterpage += 1 
           
        self.dataframe = pd.DataFrame(self.dictlist)
        self.endtime = time()
        self.exectime = self.endtime - self.starttime
        print(self.dataframe)
        print("total = {}s".format(round(self.exectime)))
        self.export()
    def getsoupdata(self):
        dict = {}
        table=[]
      
        for line in self.lines[self.counterlinepage*17 + 61:self.counterlinepage*17 + 61 + 15]:
            table.append(line.get_text(" ",strip=True))
            
        dict["transaction_ID"] = table[0]
        dict["transaction_type"] = table[1]
        dict["transaction_date"] = table[2]
        dict["transaction_status"] = table[3]
        dict["transferring_registry"] = table[4]
        dict["transferring_account_type"] = table[5]
        dict["transferring_account_name"] = table[6]
        dict["transferring_account_ID"] = table[7]
        dict["transferring_account_holder"] = table[8]
        dict["acquiring_registry"] = table[9]
        dict["acquiring_account_type"] = table[10]
        dict["acquiring_account_name"] = table[11]
        dict["acquiring_account_ID"] = table[12]
        dict["acquiring_account_holder"] = table[13]
        dict["units"] = table[14]
        
        self.dictlist.append(dict)
     
    def getnextpage(self):
        self.driver.find_element(By.XPATH,"/html/body/form/table/tbody/tr[2]/td/table/tbody/tr/td[2]/table/tbody/tr[1]/td[2]/table[3]/tbody/tr[2]/td/div/input[4]").click()
        
    def export(self):
        self.dataframe.to_csv("results.csv")
        
scrapper = scrapper()