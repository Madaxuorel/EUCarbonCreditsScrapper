from selenium import webdriver
from multiprocessing import Pool, cpu_count
from math import floor

import pandas as pd
from bs4 import BeautifulSoup
from time import *
#61:76
#composition lignes : 0-15 infos + 2 details
starttime = time()
options = webdriver.ChromeOptions()
options.add_argument('headless')

url = "https://ec.europa.eu/clima/ets/transaction.do;EUROPA_EUTLPUBLI001_PRD_JSESSIONID=C1IgnSrb9RcUm5vE6ZA66pQQc8bl7zrqNocLBdUJAFZXZ0Ni__Kj!-1038548499?languageCode=fr&startDate=&endDate=&transactionStatus=4&fromCompletionDate=&toCompletionDate=&transactionID=&transactionType=-1&suppTransactionType=-1&originatingRegistry=-1&destinationRegistry=-1&originatingAccountType=-1&destinationAccountType=-1&originatingAccountIdentifier=&destinationAccountIdentifier=&originatingAccountHolder=&destinationAccountHolder=&currentSortSettings=&nextList=Next%3E&resultList.currentPageNumber="
dictlist = []

pageCount = 100

def getPageData(n):
    dictlistTmp = []
    driver = webdriver.Chrome(executable_path="chromedriver", options = options)
    driver.get(url + str(n))
    data = driver.page_source

    print("Analyse de la page {}".format(n+1))
        
    soup = BeautifulSoup(data,"html.parser")
    tables = soup.findChildren('table')[7]
    lines = tables.findChildren(['td'])
    counterlinepage = 0
    while counterlinepage < 20:
        getsoupdata(counterlinepage, lines, dictlistTmp)
        counterlinepage += 1

    return dictlistTmp

def getsoup(dictlist):
    global dataframe
    pool = Pool()

    ITERATION_COUNT = cpu_count()-1 #-1 pour ne pas faire douiller l'ordinateur

    count_per_iteration = floor(pageCount/int(ITERATION_COUNT))

    for i in range(0, ITERATION_COUNT-1):
        for res in pool.map(getPageData, [n for n in range(count_per_iteration*i, count_per_iteration*(i+1))]):
            dictlist.extend(res)
    for res in pool.map(getPageData, [n for n in range(count_per_iteration*(ITERATION_COUNT-1), pageCount)]):
        dictlist.extend(res)
    
    pool.close()
    pool.join()        
    
    dataframe = pd.DataFrame(dictlist)
    endtime = time()
    exectime = endtime - starttime
    print(dataframe)
    print("{} pages in {}s".format(pageCount, round(exectime)))
    export()

def getsoupdata(counterlinepage, lines, dictlist):
    dict = {}
    table=[]
    
    for line in lines[counterlinepage*17 + 61:counterlinepage*17 + 61 + 15]:
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
    
    dictlist.append(dict)

def export():
    dataframe.to_csv("results.csv")

if __name__ == "__main__":
    getsoup(dictlist)