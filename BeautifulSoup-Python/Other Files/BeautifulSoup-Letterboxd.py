# -*- coding: utf-8 -*-
"""
Created on Mon Feb  5 15:44:19 2024

@author: abhis
"""

#%%
import pandas as pd
import re
import requests
from bs4 import BeautifulSoup
import json
import time
import warnings; warnings.filterwarnings("ignore");

wd = r'C:\Users\abhis\Desktop\NEU\DAMG7374 LLM w Knowledge Graph DB\\'


df = pd.read_csv(wd+"sample_data.csv")

a = df.copy()
a = a[:5]

movies = a.iloc[:,5].to_list()
movie_code = ["-".join(word for word in re.sub(r'[^\w\s]', ' ', x).lower().split()) for x in movies]
movie_link = ["https://letterboxd.com/film/"+x for x in movie_code]
#%%
data = {}
for name,link in zip(movie_code,movie_link):
    print(name)
    time.sleep(1.5)
    link = link
    r = requests.get(link, verify=False)
    if r != 200:
        print("--------------------------")
    else:
        break
    soup = BeautifulSoup(r.text, 'html.parser')
    
    
    '''
    
    
    Json of Basic data
    
    '''
    script_tag = soup.find('script', {'type': 'application/ld+json'})
    json_data = script_tag.contents[0]
    json_data = [x for x in str(json_data).split("\n") if len(x)>100][0]
    json_data = json.loads(json_data)
    
    '''
    Crew
    '''
    all_crew = soup.findAll("span",{"class":"crewrole -full"})
    all_crew = [{x.text:x.find_next('a').text} for x in all_crew]
    crew_data = {'crew': all_crew}
    json_data.update(crew_data)
    try:
        film_id = json_data['image'].split("/")[-1].split("-")[0]
        api_ott = 'https://letterboxd.com/s/film-availability?filmId='+film_id+'&locale=USA'
        time.sleep(1.5)
    except:
        pass
    
    '''
    
    
    1. To find crew details
    2. To find Other Details
    3. To find OTT
    '''
    
    #l = 'https://letterboxd.com/s/film-availability?filmId=104174&locale=USA'# ID needs to be changed

    ott = json.loads(requests.get(api_ott,verify=False).text)
    ott = {"stream":ott}
    json_data.update(ott)

    data[name] = json_data

