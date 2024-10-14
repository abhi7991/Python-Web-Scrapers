# -*- coding: utf-8 -*-
"""
Created on Fri Apr 17 10:19:50 2020

@author: HP
"""

from selenium import webdriver
import sys
import re
import os
import sqlalchemy
from selenium.common.exceptions import NoSuchElementException
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import datetime
from pytz import timezone
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import pickle
#%%
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days

job_start_time = datetime.datetime.now(india_time)
table_name = 'AGMARKNET_DATA'
scheduler = ''
#no_of_ping = 0

def input_fun(driver, st, date):
#    global no_of_ping
    mon = date.strftime("%B")
    year = date.year

    st_xpath = '//select[@name="ctl00$cphBody$cboState"]'
    time.sleep(1)
#    no_of_ping += 1
    driver.find_elements_by_xpath(st_xpath + "//option[contains(text(), '" + st + "')]")[0].click()
    time.sleep(2)

    mon_xpath = '//select[@name="ctl00$cphBody$cboMonth"]'
    time.sleep(1)
#    no_of_ping += 1
    driver.find_elements_by_xpath(mon_xpath + '//option[contains(text(), "' + mon + '")]')[0].click()
    time.sleep(2)

    year_xpath = '//select[@name="ctl00$cphBody$cboYear"]'
    time.sleep(1)
#    no_of_ping += 1
    driver.find_elements_by_xpath(year_xpath + '//option[contains(text(), "' + str(year) + '")]')[0].click()
    time.sleep(2)

    return driver
def clean_up(chrome,st):

        html=chrome.page_source
        df1=pd.read_html(html)
        dsoup=BeautifulSoup(html,'html.parser')
        select = dsoup.find('font', {'color':'Maroon'})
        date=select.contents[0].strip()
        df=df1[3]
#        cond1=(df['Market'].str.lower().str.contains('group')==False) & ((df['Market']==df['Arrivals'])==True)
#        cond2=(df['Market'].str.lower().str.contains('group')==True) & ((df['Market']==df['Arrivals'])==True)
#        df['Product']=np.where(cond1,df['Market'],None)
#        df['Group']=np.where(cond2,df['Arrivals'],None)
#        col=['Product','Group']
#        df[col]=df.loc[:,col].ffill()
        df2 = df.copy()
        df2 = df2.iloc[:,0:len(df.columns)-3]
        df2.dropna(axis=0,inplace=True)
        df=df.drop_duplicates(keep=False)
        df = df.query("Market != Arrivals")
        new_cols = df.columns.to_list()
        new_cols = [x.replace(" ",'_').replace("-","_") for x in new_cols]
        df.columns = new_cols
        format_str = '%d/%m/%Y' # The format
        datetime_obj = datetime.datetime.strptime(date, format_str)
        df=df.rename(columns={'Arrivals':'Arrivals_String'})
        df['Arrivals']=np.where(df['Arrivals_String']=='NR',0,df['Arrivals_String'])
        df['Relevant_Date']=datetime_obj.strftime("%Y-%m-%d")
        df['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
        df['Last_Updated'] = ''
        df['State']=st
        df[['Arrivals','Minimum_Prices', 'Maximum_Prices','Modal_Prices']]=df[['Arrivals','Minimum_Prices', 'Maximum_Prices','Modal_Prices']].apply(pd.to_numeric,errors='coerce')
        df['Relevant_Date'] =  pd.to_datetime(df['Relevant_Date'], format='%Y-%m-%d')
        df=df[['State','Market', 'Arrivals_String','Arrivals', 'Unit_of_Arrivals', 'Variety', 'Minimum_Prices',
               'Maximum_Prices', 'Modal_Prices', 'Unit_of_Price'
               , 'Relevant_Date', 'Runtime', 'Last_Updated']]
#        with open(r"Kmeans-cluster.pickle", "rb") as input_file:
#             e = pickle.load(input_file)
#             print(e)

        return df


#%%
#def run_program(run_by='Adqvest_Bot', py_file_name=None):
#    global no_of_ping
#    if(py_file_name is None):
#        py_file_name = sys.argv[0].split('.')[0]
#
#    try:
#        if(run_by=='Adqvest_Bot'):
#            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
#        else:
#            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
start = time.process_time()
main_limit=0
while True:
    try:
        start = time.process_time()
        site_url = "https://agmarknet.gov.in/PriceAndArrivals/CommodityDailyStateWise.aspx"
        wd = r'C:\Personal_Project\Web_Scraping_Stuff\Selenium'
        driver = webdriver.Chrome(executable_path = wd+"\chromedriver.exe")
#        no_of_ping += 1
        driver.get(site_url)
        driver.maximize_window()
        eles = driver.find_elements_by_xpath('//select[@name="ctl00$cphBody$cboState"]')
#                states = eles[0].text.split('\n')
        r = requests.Session()
        sesh1=r.get(site_url)
        soup1 = BeautifulSoup(sesh1.content , "lxml")
        states=[]
        select = soup1.find('select', id="cphBody_cboState")
        for value in select.stripped_strings:
            print (value)
            states.append(value)
        states = states[1:]  
        states1 = ['Tamil Nadu','Kerala','Andhra Pradesh','Karnataka','Telangana','Pondycherry']
        states = [elem for elem in states if elem not in states1 ]
        states = [st.strip() for st in states if(("select" not in st.lower()) & (st.strip()!=''))]#list of states
        states = [states[0]]
        main=pd.DataFrame()
        all_dates = []
        st_mon = 4
        st_year = 2022
        #list of dates loop
        while True:
            if((st_mon==today.month) & (st_year==today.year)):
                break
            if(st_mon==13):
                st_mon = 1
                st_year += 1
            all_dates.append(datetime.date(st_year, st_mon, 1))
            st_mon += 1


        for st in states:
            print(st)
            for date in [all_dates[0]]:
                print(st, date)
                driver = input_fun(driver, st, date)
                driver.implicitly_wait(10)
                xpath = '//*[@id="cphBody_Calendar1"]/tbody//a'
                eles = driver.find_elements_by_xpath(xpath)
                all_text = []
                for ele in eles:
                    all_text.append(ele.text)

                tracker_all_dates = []
                for text in all_text:
                    rel_date = datetime.date(date.year, date.month, int(text))
                    tracker_all_dates.append(rel_date)

                state_done = pd.DataFrame(data=tracker_all_dates, columns=['Relevant_Date'])
                state_done['State'] = st
                for text in [all_text[0]]:
                    code_rel_date = datetime.date(date.year, date.month, int(text))
                    print(code_rel_date)
                    driver = input_fun(driver, st, date)
                    print(text)
                    rel_date = date.strftime("%B %Y")
                    time.sleep(1)
                    limit = 0
                    while True:
                        try:
                            cal = driver.find_elements_by_xpath('//*[@id="cphBody_Calendar1"]/tbody/tr[1]/td/table/tbody//td')[0].text
                            if(cal.strip()==rel_date):
                                break
                            else:
                                raise Exception("Date Mismatch")
                        except:
                            limit += 1
                            if(limit>5):
                                raise Exception("Internet slow")
                            time.sleep(2)
                    ele = driver.find_elements_by_xpath(xpath + '[text() = "' + text + '"]')[0]
                    ele.click()
                    time.sleep(2)
                    
                    try:                        
                        xpath_submit='//*[@id="cphBody_btnSubmit"]'
                        time.sleep(3)
                        driver.implicitly_wait(10) # seconds
                        submit = driver.find_element_by_xpath(xpath_submit)
                        submit.click()
                        driver.implicitly_wait(5)
                    except NoSuchElementException:  
                        runtime=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#                        query = "update AGMARKNET_TRACKER set Date_Status='Failed',Runtime='" + str(runtime) + "' where State='" + st + "' and Relevant_Date='" + str(code_rel_date) + "'"
#                        connection.execute(query)
#                        connection.execute('commit')
                        continue                  
                    time.sleep(2)
                    output=clean_up(driver,st)
                    print(output.head())
#                    output.to_sql(name='AGMARKNET_PAST_DATA',con=engine,if_exists='append',index=False)
                    runtime=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#                    query = "update AGMARKNET_TRACKER set Date_Status='Done',Runtime='" + str(runtime) + "' where State='" + st + "' and Relevant_Date='" + str(code_rel_date) + "'"
#                    connection.execute(query)
#                    connection.execute('commit')
                    driver.get(site_url)
#            query = "update AGMARKNET_TRACKER set State_Status='Done' where State='" + st + "'"
#            connection.execute(query)
#            connection.execute('commit')
#        query = "delete from AGMARKNET_TRACKER"
#        connection.execute(query)
#        connection.execute('commit')
#        time.sleep(2)
        print(time.process_time() - start)
#        log.job_end_log(table_name,job_start_time, no_of_ping)
        try:
            driver.quit()
        except:
            pass

        break

    except:
        try:
            driver.quit()
        except:
            pass
        main_limit += 1
        if(main_limit>4):
            error_msg = str(sys.exc_info()[1])
            raise Exception(error_msg)
        time.sleep(5)

#    except:
#        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
#        error_msg = str(sys.exc_info()[1])
#        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)
#
#if(__name__=='__main__'):
#    run_program(run_by='manual')
