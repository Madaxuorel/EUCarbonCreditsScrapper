from selenium import webdriver
from multiprocessing import Pool, cpu_count
from math import floor

import pandas as pd
from bs4 import BeautifulSoup
from time import *

options = webdriver.ChromeOptions()
options.add_argument('headless')
url = "https://ec.europa.eu/clima/ets/ohaDetails.do?accountID=109653&action=all&languageCode=en&returnURL=resultList.currentPageNumber%3D1%26installationName%3D%26accountHolder%3D%26search%3DSearch%26permitIdentifier%3D%26form%3Doha%26searchType%3Doha%26mainActivityType%3D38%26currentSortSettings%3D%26installationIdentifier%3D%26languageCode%3Den&registryCode=FR"
#ligne 54
def getpage():
    driver=webdriver.Chrome(executable_path="chromedriver", options=options)
    driver.get(url)
    data=driver.page_source
    
    soup = BeautifulSoup(data,"html.parser")
    operator_info(soup)
    compliance_info(soup,False) #ne fonctionne que pour les operateurs aériens
    
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
        print(df)
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
        print(df)
        
def operator_info(soup):
    
    tables = soup.findChildren('table')[5]
    lines = tables.findChildren(['td'])[54]
    dfs = pd.read_html(lines.findChildren("td")[2].prettify())
    holding_infos = dfs[0]
    holding_infos.drop(index=0,axis=1,inplace=True)
    contact_info = dfs[1]
    contact_info.drop(index=0,axis=1,inplace=True)
    contact_info.drop(index=1,axis=1,inplace=True)
    
    
    dfs = pd.read_html(lines.findChildren("td")[40].prettify())
    operator_infos = dfs[0]
    operator_infos.drop(index=0,axis=1,inplace=True)
    adress = dfs[1]
    adress.drop(index=0,axis=1,inplace=True)
    print(holding_infos)
    print(contact_info)
    print(operator_infos)
    print(adress)
    
    
getpage()