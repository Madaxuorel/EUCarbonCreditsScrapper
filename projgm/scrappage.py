import requests
from multiprocessing import Pool, cpu_count
from math import floor

import pandas as pd
from bs4 import BeautifulSoup
from time import *


url = "https://ec.europa.eu/clima/ets/oha.do?form=oha&languageCode=en&accountHolder=&installationIdentifier=&installationName=&permitIdentifier=&mainActivityType=-1&searchType=oha&currentSortSettings=&nextList=Next%3E&resultList.currentPageNumber="
fromPage = 0
toPage = 1
links = []

#ligne 54
def getpage(link, isAirline):
    data=requests.get(link)
    
    soup = BeautifulSoup(data.text,"html.parser")
    op_info = operator_info(soup)
    compliance = compliance_info(soup,isAirline) #ne fonctionne que pour les operateurs aériens
    
def compliance_info(soup,is_airline):
    if is_airline:
        tables = soup.findChildren('table')[5]
        lines = tables.findChildren(['td'])[54]
        df = pd.read_html(lines.findChildren("td")[81].prettify())[0]
        df.drop(columns=9,axis=1,inplace=True)
        df.drop(columns=8,axis=1,inplace=True)
        df.drop(index=0,axis=0,inplace=True)
        df.drop(index=1,axis=0,inplace=True)
        df.drop(index=28,axis=0,inplace=True)
        df.drop(index=29,axis=0,inplace=True)
        df.drop(index=30,axis=0,inplace=True)
        df.drop(index=31,axis=0,inplace=True)
        df.drop(index=32,axis=0,inplace=True)
        
    else:
        tables = soup.findChildren('table')[5]
        lines = tables.findChildren(['td'])[54]
        df = pd.read_html(lines.findChildren("td")[79].prettify())[0]
        df.drop(columns=9,axis=1,inplace=True)
        df.drop(columns=8,axis=1,inplace=True)
        df.drop(index=0,axis=0,inplace=True)
        df.drop(index=1,axis=0,inplace=True)
        df.drop(index=28,axis=0,inplace=True)
        df.drop(index=29,axis=0,inplace=True)
        df.drop(index=30,axis=0,inplace=True)
        df.drop(index=31,axis=0,inplace=True)
        df.drop(index=32,axis=0,inplace=True)
        df.drop(index=33,axis=0,inplace=True)
        
    total=[]
    
    for i in range(2,28):
        
        total.append(df.loc[i, :].values.flatten().tolist())
    
    return df
     
    
def operator_info(soup):
    
    tables = soup.findChildren('table')[5]
    lines = tables.findChildren(['td'])[54]
    dfs = pd.read_html(lines.findChildren("td")[2].prettify())
    holding_infos = dfs[0]
    holding_infos.drop(index=0,axis=1,inplace=True)
    holding_infos.reset_index(drop=True, inplace=True)
    holding_infos.drop(columns=6,inplace=True)
    contact_info = dfs[2]
    contact_info.drop(index=0,axis=1,inplace=True)
    contact_info.drop(index=1,axis=1,inplace=True)
    contact_info.reset_index(drop=True, inplace=True)
    
    dfs = pd.read_html(lines.findChildren("td")[40].prettify())
    operator_infos = dfs[0]
    operator_infos.drop(index=0,axis=1,inplace=True)
    operator_infos.drop(columns=10,inplace=True)
    adress = dfs[1]
    adress.drop(index=0,axis=1,inplace=True)
    adress.drop(columns=10,inplace=True)
    adress.drop(columns=9,inplace=True)
    adress.drop(columns=8,inplace=True)

    concat_1 = pd.concat([holding_infos, contact_info], axis=1)
    concat_2 = pd.concat([operator_infos,adress],axis=1)
    concat_2.reset_index(drop=True,inplace=True)
    concat_final = pd.concat([concat_1,concat_2],axis=1)
    row_list = concat_final.loc[1, :].values.flatten().tolist()

    return row_list
    
    
for i in range(fromPage, toPage):
    data = requests.get(url + str(i))
    soup = BeautifulSoup(data.text,"html.parser")
    button = soup.find_all('a', {'class': 'listlink'})
    for b in button:
        if "Details - All Phases" in b.text:
            tmp = b.parent.parent.parent.parent.parent
            links.append((b['href'], "Aircraft operator activities" in tmp.text))
for link, isAirline in links[0:len(links)]:
    getpage(link, isAirline)