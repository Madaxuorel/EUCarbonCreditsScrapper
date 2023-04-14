import requests
from multiprocessing import Pool, cpu_count
from math import floor
import pandas as pd
from bs4 import BeautifulSoup
from requests.exceptions import ConnectionError
from http.client import RemoteDisconnected
from time import *
import math
from csv import writer

url = "https://ec.europa.eu/clima/ets/oha.do?form=oha&languageCode=en&accountHolder=&installationIdentifier=&installationName=&permitIdentifier=&mainActivityType=-1&searchType=oha&currentSortSettings=&nextList=Next%3E&resultList.currentPageNumber="
fromPage = 1
toPage = 897

links = []
totalcrashes = 0
totallinks = 0
done = []
CH_countries = ["Belgium", "Croatia", "Finland", "France", "Allemagne", "Greece", "Hungary", "Iceland", "Ireland", "Italy",
                "Latvia", "Lituania", "Malta", "Netherlands", "Poland", "Portugal", "Romania", "Slovakia", "Sweden", "United Kingdom"]


def write(op_info: list, compliance: list, CH: list):
    """Fonction pour écrire le résultat dans le csv du résultat"""
    op_info.append(compliance if compliance is not None else [])
    op_info.append(CH if CH is not None else [])
    with open("results2.csv", mode="a+", newline='') as csv:
        writer(csv, delimiter=",").writerow(op_info)


def getpage(link: str, isAirline: bool, is_CH: bool):
    """Fonction permettant de récupérer le contenu d'une page Holding"""
    # check si la page n'as pas déjà été scrappée
    if link in done:
        return
    # code permettant d'essayer en boucle jusqu'à ce que la page soit récupérée
    while True:
        try:
            data = requests.get(link)
            if data.status_code == 200:
                break
            else:
                sleep(1)
        except:
            print("error")

    done.append(link)
    # récupération du contenu de la page
    soup = BeautifulSoup(data.text, "html.parser")
    op_info = operator_info(soup, link)
    compliance = compliance_info(soup, isAirline)
    if is_CH and isAirline:
        CH = CH_scrap(soup)
    else:
        CH = ""
    write(op_info if op_info is not None else [], compliance, CH)


def compliance_info(soup: BeautifulSoup, is_airline: bool) -> str:
    """Fonction permettant de récupérer les données de la Holding suivant si c'est une airline ou non"""
    if is_airline:
        tables = soup.findChildren('table')[5]
        lines = tables.findChildren(['td'])[54]
        try:
            df = pd.read_html(lines.findChildren("td")[81].prettify())[0]
        except:
            print("got a problem")
            return
        df.drop(columns=9, axis=1, inplace=True)
        df.drop(columns=8, axis=1, inplace=True)
        df.drop(index=0, axis=0, inplace=True)
        df.drop(index=1, axis=0, inplace=True)
        df.drop(index=28, axis=0, inplace=True)
        df.drop(index=29, axis=0, inplace=True)
        df.drop(index=30, axis=0, inplace=True)
        df.drop(index=31, axis=0, inplace=True)
        df.drop(index=32, axis=0, inplace=True)

    else:
        tables = soup.findChildren('table')[5]
        lines = tables.findChildren(['td'])[54]
        try:
            df = pd.read_html(lines.findChildren("td")[79].prettify())[0]
        except:
            print("got a problem")
            return
        df.drop(columns=9, axis=1, inplace=True)
        df.drop(columns=8, axis=1, inplace=True)
        df.drop(index=0, axis=0, inplace=True)
        df.drop(index=1, axis=0, inplace=True)
        df.drop(index=28, axis=0, inplace=True)
        df.drop(index=29, axis=0, inplace=True)
        df.drop(index=30, axis=0, inplace=True)
        df.drop(index=31, axis=0, inplace=True)
        df.drop(index=32, axis=0, inplace=True)
        df.drop(index=33, axis=0, inplace=True)

    total = []
    totall = []
    totalll = []
    for i in range(2, 28):
        total.append((df.loc[i, :].values.flatten().tolist()))

    for row in total:
        totall.append([str(element) for element in row])

    for row in totall:
        totalll.append(";".join(row))

    return "/".join(totalll)


def CH_scrap(soup: BeautifulSoup) -> str:
    """Récupération spécifique pour certains pays car structure de la page différente"""
    tables = soup.findChildren('table')[5]
    try:
        df = pd.read_html(tables.findChildren(["td"])[56].prettify())[8]
    except:
        print("got a problem")
        return
    df.drop(index=28, axis=0, inplace=True)
    df.drop(index=29, axis=0, inplace=True)
    df.drop(index=30, axis=0, inplace=True)
    df.drop(index=31, axis=0, inplace=True)
    df.drop(index=32, axis=0, inplace=True)
    df.drop(index=0, axis=0, inplace=True)
    df.drop(index=1, axis=0, inplace=True)
    df.drop(columns=9, axis=1, inplace=True)
    df.drop(columns=8, axis=1, inplace=True)

    total = []
    totall = []
    totalll = []

    for i in range(2, 28):
        total.append((df.loc[i, :].values.flatten().tolist()))

    for row in total:
        totall.append([str(element) for element in row])

    for row in totall:
        totalll.append(";".join(row))
    return "/".join(totalll)


def operator_info(soup: BeautifulSoup) -> list:

    tables = soup.findChildren('table')[5]
    lines = tables.findChildren(['td'])[54]
    dfs = pd.read_html(lines.findChildren("td")[2].prettify())
    holding_infos = dfs[0]
    holding_infos.drop(index=0, axis=1, inplace=True)
    holding_infos.reset_index(drop=True, inplace=True)
    holding_infos.drop(columns=6, inplace=True)
    contact_info = dfs[2]
    contact_info.drop(index=0, axis=1, inplace=True)
    contact_info.drop(index=1, axis=1, inplace=True)
    contact_info.reset_index(drop=True, inplace=True)

    try:
        dfs = pd.read_html(lines.findChildren("td")[40].prettify())
    except:
        print("got a problem")
        return
    operator_infos = dfs[0]
    operator_infos.drop(index=0, axis=1, inplace=True)
    operator_infos.drop(columns=10, inplace=True)
    adress = dfs[1]
    adress.drop(index=0, axis=1, inplace=True)
    adress.drop(columns=10, inplace=True)
    adress.drop(columns=9, inplace=True)
    adress.drop(columns=8, inplace=True)

    concat_1 = pd.concat([holding_infos, contact_info], axis=1)
    concat_2 = pd.concat([operator_infos, adress], axis=1)
    concat_2.reset_index(drop=True, inplace=True)
    concat_final = pd.concat([concat_1, concat_2], axis=1)
    row_list = concat_final.loc[1, :].values.flatten().tolist()

    return row_list


def is_in(liste: list, string: str) -> bool:
    """Check if string is in one of the element of the list"""
    for e in liste:
        if e in string:
            return True
    return False


def processPage(i: int):
    """process each page to be done"""
    global totallinks, totalcrashes
    while True:
        try:
            data = requests.get(url + str(i))
            if data.status_code == 200:
                break
            else:
                sleep(1)
        except:
            totalcrashes += 1
            print("connection error, total crashes = {}".format(totalcrashes))
    soup = BeautifulSoup(data.text, "html.parser")
    button = soup.find_all('a', {'class': 'listlink'})
    # find the link associated to the button
    for b in button:
        if "Details - All Phases" in b.text:
            tmp = b.parent.parent.parent.parent.parent

            links.append((b['href'], "Aircraft operator activities" in tmp.text, is_in(
                CH_countries, tmp.text)))

    for link, isAirline, is_CH in links[0:len(links)]:
        totallinks += 1

        getpage(link, isAirline, is_CH)


if __name__ == "__main__":
    pool = Pool()
    # save as in scrap.py
    ITERATION_COUNT = cpu_count()-1

    count_per_iteration = floor((toPage-fromPage)/int(ITERATION_COUNT))

    for i in range(0, ITERATION_COUNT-1):
        pool.map(processPage, [n for n in range(
            count_per_iteration*i+fromPage, count_per_iteration*(i+1)+fromPage)])
    pool.map(processPage, [n for n in range(
        count_per_iteration*(ITERATION_COUNT-1)+fromPage, toPage)])

    pool.close()
    pool.join()
