from multiprocessing import Pool, cpu_count
from math import floor
import requests

import pandas as pd
from bs4 import BeautifulSoup
from time import *
#61:76
#composition lignes : 0-15 infos + 2 details

url = "https://ec.europa.eu/clima/ets/transaction.do;EUROPA_EUTLPUBLI001_PRD_JSESSIONID=C1IgnSrb9RcUm5vE6ZA66pQQc8bl7zrqNocLBdUJAFZXZ0Ni__Kj!-1038548499?languageCode=fr&startDate=&endDate=&transactionStatus=4&fromCompletionDate=&toCompletionDate=&transactionID=&transactionType=-1&suppTransactionType=-1&originatingRegistry=-1&destinationRegistry=-1&originatingAccountType=-1&destinationAccountType=-1&originatingAccountIdentifier=&destinationAccountIdentifier=&originatingAccountHolder=&destinationAccountHolder=&currentSortSettings=&nextList=Next%3E&resultList.currentPageNumber="

limit = 100

def getPageData(n):
    dictlistTmp = []
    try:
        data = requests.get(url + str(n))
    except:
        print(f"error on page {n}")
        return dictlistTmp
    soup = BeautifulSoup(data.text,"html.parser")
    tables = soup.findChildren('table')[7]
    lines = tables.findChildren(['td'])
    counterlinepage = 0
    while counterlinepage < 20:
        getsoupdata(counterlinepage, lines, dictlistTmp)
        counterlinepage += 1

    return dictlistTmp

def getsoup(dictlist, startPage, endPage):
    global dataframe
    pool = Pool()

    ITERATION_COUNT = cpu_count()-1 #-1 pour ne pas faire douiller l'ordinateur

    count_per_iteration = floor((endPage-startPage)/int(ITERATION_COUNT))

    for i in range(0, ITERATION_COUNT-1):
        for res in pool.map(getPageData, [n for n in range(count_per_iteration*i+startPage, count_per_iteration*(i+1)+startPage)]):
            if res != []: dictlist.extend(res)
    for res in pool.map(getPageData, [n for n in range(count_per_iteration*(ITERATION_COUNT-1)+startPage, endPage)]):
        if res != []: dictlist.extend(res)
    
    pool.close()
    pool.join()        
    
    dataframe = pd.DataFrame(dictlist)
    export(f"{startPage}-{endPage}")

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

def export(name):
    dataframe.to_csv(name + ".csv")

if __name__ == "__main__":
    starttime = time()
    for i in range(0, limit, 100):
        dictlist = []
        print(f"processing batch {i} to {i+100}")
        getsoup(dictlist, i, i+100)
    endtime = time()
    exectime = endtime - starttime
    print(f"{limit} pages done in {exectime//60} mn et {exectime%60} s !")