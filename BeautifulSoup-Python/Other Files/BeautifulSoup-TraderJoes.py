# -*- coding: utf-8 -*-
"""
Created on Tue Feb 13 19:44:30 2024

@author: abhis
"""

import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import time
url = 'https://locations.traderjoes.com/'
r = requests.get(url, verify=False)

soup = BeautifulSoup(r.text, 'html.parser')
state_link = soup.findAll('a',{'class':'ga_w2gi_lp listitem'})

data = pd.DataFrame()

state_links = soup.findAll('a',{'class':'ga_w2gi_lp listitem'})

for sl in state_links:
    s_link = sl.get("href")
    s_name = sl.get('data-galoc')

    time.sleep(2)
    r = requests.get(s_link, verify = False)

    soup = BeautifulSoup(r.text, 'html.parser')

    c_link = soup.findAll('a',{'class':'ga_w2gi_lp listitem'})
    c_link = [x.get("href") for x in c_link]
    for cl in c_link:
        r = requests.get(cl, verify=False,timeout=(3.05, 27))
        soup = BeautifulSoup(r.text, 'html.parser')
        time.sleep(2)
        #r = requests.get('https://github.com', timeout=(3.05, 27))
        json_content = json.loads(soup.findAll('script',{"type":'application/ld+json'})[1].contents[0])
        
        df = pd.DataFrame(json_content['address'],index=[0])
        df['URL'] = cl
        df['Name'] = json_content['name']
        df['State'] = s_name
        data = pd.concat([data,df])
        print(s_name,json_content['name'])
        
data.to_csv("C:\\Personal_Project\\TJ_Location.csv",index=False)

