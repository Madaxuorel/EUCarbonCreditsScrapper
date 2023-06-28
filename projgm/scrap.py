from multiprocessing import Pool, cpu_count
from math import floor
import requests

import pandas as pd
from bs4 import BeautifulSoup
from time import *
# 61:76
# composition lignes : 0-15 infos + 2 details

# url de la page à scrapper
url = "https://ec.europa.eu/clima/ets/transaction.do;EUROPA_EUTLPUBLI001_PRD_JSESSIONID=C1IgnSrb9RcUm5vE6ZA66pQQc8bl7zrqNocLBdUJAFZXZ0Ni__Kj!-1038548499?languageCode=fr&startDate=&endDate=&transactionStatus=4&fromCompletionDate=&toCompletionDate=&transactionID=&transactionType=-1&suppTransactionType=-1&originatingRegistry=-1&destinationRegistry=-1&originatingAccountType=-1&destinationAccountType=-1&originatingAccountIdentifier=&destinationAccountIdentifier=&originatingAccountHolder=&destinationAccountHolder=&currentSortSettings=&nextList=Next%3E&resultList.currentPageNumber="

# spécification de l'intervalle des pages à scrapper
lower = 0
limit = 58526


def getPageData(n: int) -> list:
    """Fonction utilisée pour récupérer le contenu de chaque page de la recherche"""
    dictlistTmp = []
    # boucle pour s'assurer qu'on récupère bien la page
    while True:
        try:
            data = requests.get(url + str(n))
            if data.status_code == 200:
                break
            else:
                sleep(1)
                print(f"wait on page {n}")
        except:
            print(f"error on page {n}")
            sleep(1)
    # extraction des lignes de la page
    soup = BeautifulSoup(data.text, "html.parser")
    tables = soup.findChildren('table')[7]
    lines = tables.findChildren(['td'])
    # pour chaque ligne, on ajoute ses données à la liste
    for i in range(20):
        dictlistTmp = getsoupdata(i, lines, dictlistTmp)

    return dictlistTmp


def getsoup(dictlist: list, startPage: int, endPage: int):
    """Fonction qui réparti la charge de travail entre chaque thread et qui enregistre le résultat"""
    pool = Pool()

    # Nombre de threads :-1 pour ne pas faire douiller l'ordinateur
    THREAD_COUNT = cpu_count()-1

    # nombre de pages à traiter par thread
    count_per_iteration = floor((endPage-startPage)/int(THREAD_COUNT))

    # répartition de la charge de travail suivant chaque thread
    for i in range(0, THREAD_COUNT-1):
        for res in pool.map(getPageData, [n for n in range(count_per_iteration*i+startPage, count_per_iteration*(i+1)+startPage)]):
            if res != []:
                dictlist.extend(res)
    # le dernier thread prend le "reste" car le nombre de pages à traiter n'est pas forcément proportionnel au nombre de threads
    for res in pool.map(getPageData, [n for n in range(count_per_iteration*(THREAD_COUNT-1)+startPage, endPage)]):
        if res != []:
            dictlist.extend(res)

    pool.close()
    pool.join()

    # sauvegarde des données dans un fichier csv
    dataframe = pd.DataFrame(dictlist)
    dataframe.to_csv(f"{startPage}-{endPage}.csv")


def getsoupdata(counterlinepage: int, lines: list, dictlist: list) -> list:
    """Fonction qui sauvegarde les donées d'une ligne"""
    dict = {}
    table = []

    for line in lines[counterlinepage*17 + 61:counterlinepage*17 + 61 + 15]:
        table.append(line.get_text(" ", strip=True))

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
    return dictlist


if __name__ == "__main__":
    starttime = time()
    # découpage de la charge de travail en batch de 100
    for i in range(lower, limit, 100):
        dictlist = []
        print(f"processing batch {i} to {i+100}")
        getsoup(dictlist, i, i+100)
    endtime = time()
    exectime = endtime - starttime
    print(f"{limit-lower} pages done in {exectime//60} mn et {exectime%60} s !")
