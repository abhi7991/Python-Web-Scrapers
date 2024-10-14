import scrapy
from scrapy import Selector

import sqlalchemy
import pandas as pd
from pandas.io import sql
import time

import os
import requests
import json
from bs4 import BeautifulSoup
from collections import OrderedDict
import re
import ast
import datetime as datetime
from pytz import timezone
import requests, zipfile, io
import csv
import numpy as np
import sys
#sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import unidecode
sys.path.insert(0, r'C:/Adqvest/Adqvest_Function')
from quantities import units
import pandahouse as ph
#from clickhouse_driver import Client
#import ClickHouse_db

#DB Connection

conndetail = pd.read_csv('C:/Adqvest/Amazon_AdQvest_properties.txt',delim_whitespace=True)
#conndetail = pd.read_csv('Amazon_AdQvest_properties.txt',delim_whitespace=True)
#os.chdir('C:/Users/Administrator/AdQvestDir')  # 'C:/Users/Administrator/Documents'
#os.environ["http_proxy"] = "http://localhost:12345"

#DB Connection
hostdet = conndetail.loc[conndetail['Env'] == 'Host']
port = conndetail.loc[conndetail['Env'] == 'port']
DBname = conndetail.loc[conndetail['Env'] == 'DBname']
host = list(hostdet.iloc[:,1])
port = list(port.iloc[:,1])
dbname = list(DBname.iloc[:,1])
Connectionstring = 'mysql+pymysql://' + host[0] + ':' + port[0] + '/' + dbname[0]

engine = sqlalchemy.create_engine(Connectionstring)
#client = ClickHouse_db.db_conn()
connection = engine.connect()
india_time = timezone('Asia/Kolkata')
today = datetime.datetime.now(india_time)
yesterday = datetime.datetime.now(india_time) - datetime.timedelta(1)

tab_name = ['AMAZON_HOME_AND_KITCHEN','AMAZON_BABY_PRODUCTS','AMAZON_SHOES_AND_HANDBAGS','AMAZON_HEALTH_AND_PERSONAL_CARE','AMAZON_WATCHES','AMAZON_GROCERY_AND_GOURMET_FOODS','AMAZON_CAR_AND_MOTERBIKE','AMAZON_BEAUTY','AMAZON_LAUNCHPAD']    
amaz_name = ['Home & Kitchen','Baby Products','Shoes & Handbags','Health & Personal Care','Watches','Grocery & Gourmet Foods','Car & Motorbike', 'Beauty','Amazon Launchpad']
Name_Matches = pd.DataFrame({"Table_Name":tab_name,"Amazon_Name":amaz_name})

def Min_Price(x):
    if(len(x)==0):
        return np.nan
    elif(len(x)==1):
        return x[0]
    else:
        return x[0]

def Max_Price(x):
    if(len(x)==0):
        return np.nan
    elif(len(x)==1):
        return x[0]
    else:
        return x[1]

def f(x):
    if (len(x)>0):
        return(x[0])
    else:
        return('')

def extractPiecesAndSku(myDf):
    ''' (pandas.DataFrame) -> (pandas.DataFrame)
        extracts qty and sku from product name
    '''
    import gc

    # converting to lower case
    myDf['Name'] = myDf['Name'].str.lower()

    # varibles for different units
    sku_units = 'milliliters|ml|mg|kg|kilogram|gram|gr|g|lb|ltr|lt|l|fl. oz|oz|ounce|fluid ounce|ounces|kilo'
    dim_units = 'inches|inch|cm|mm|m|in|foot|feet|ft|f'
    qty_units = 'pieces|pc|units|pairs|singles|tea bags|envolopes|bunch|bunches|envolope|count|teabox|teabags|teabags|bag|pouches|pouch|tabs|candies|pellets|pellet|leaves|leave|pods|pod|combo|tablets|tablet|hamper|count|wipes|box|sheets|packs|slab|cup|cups|sachet|pallets|set|pack|tins|tin|bars|bar|jars|jar|sticks|stick|sachets|sachet|pcs|pbottles|bottle'

    # regex patterns
    # strings like , 0.25gm etc. (sku)
    pat_sku = f'(\(?,?\s?(?P<sku>[0-9]*\.?[0-9]+?)\)?\s?-?\s?_?(?P<sku_units>{sku_units})\s?)'

    # strings like pack of 10, pack 23, (qty)
    pat_qty_1 = f'(?:\(?_?-?\s?(?P<qty_units>{qty_units})_?-?:?\s?(?:of)?_?-?\s(?P<qty>[0-9]+)\)?)' # FIXED BUG

    # strings like 10 pieces etc. 88 (qty)
    pat_qty_2 = f'(?:\(?_?-?\s?(?P<qty>[0-9]+)_?-?\s?(?P<qty_units>{qty_units})\)?)'

    # strings like (200gm x 3) (qty)
    pat_qty_3 = f'\(?(?:[0-9]*\.?[0-9]+?)\s?(?:{sku_units})\s?x\s?(?P<qty>[0-9]+)\)?'

    # strings like (24 x 13g) (qty)
    pat_qty_4 = f'\(?\s?(?P<qty>[0-9]+)\s?x\s?(?:[0-9]*\.?[0-9]+?)\s?(?:{sku_units})\)?'

    ################# SKU #################

    nameList = myDf['Name'].str.extract(pat = pat_sku)

    # inserted into dataframe
    myDf['SKU'] = nameList['sku']
    myDf['SKU_Units'] = nameList['sku_units']

    del nameList
    gc.collect()

    ################# QTY #################

    # myDf to store results
    results = pd.DataFrame(np.ones(len(myDf)))
    results.rename(columns={0:'qty'}, inplace=True)
    # results['qty_units'] = ''
    # results['qty'] = results['qty'].astype(int)

    patList = [pat_qty_1, pat_qty_2, pat_qty_3, pat_qty_4]

    for myPat in patList:
        # print(myPat)
        qtyDf = myDf['Name'].str.extract(pat = myPat).dropna()
        if myPat in [pat_qty_3, pat_qty_4]:
            qtyDf['qty_units'] = 'pieces'
        qtyDf['qty'] = qtyDf['qty'].apply(lambda x: int(x))

        # replace items on those indexes
        results.loc[qtyDf.index, 'qty'] = qtyDf['qty']
        results.loc[qtyDf.index, 'qty_units'] = qtyDf['qty_units']

    # results['qty_units'] = results['qty_units'].apply(lambda x: 'pieces' if x == '' else x)
    results['qty_units'] = results['qty_units'].fillna('piece')

    # innserting qty and qty_unit into myDf
    myDf['QTY'] = results['qty'].apply(lambda x: str(x))
    myDf['QTY_Units'] = results['qty_units']

    del results, qtyDf
    gc.collect()

    return myDf

class CarSpider(scrapy.Spider):
    name = "amazon_catg_links"


    def start_requests(self):
        header = {'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}
        self.dict = {}
        self.dict['Link'] = []
        self.dict['Category'] = []
        self.dict['Sub_Category_1'] = []

        query = "select max(Relevant_Date) as RD from AdqvestDB.AMAZON_BESTSELLERS_LINKS"
        Date = pd.read_sql(query, con=engine).iloc[0,0].strftime("%Y-%m-%d")
        urls = pd.read_sql("Select * from AMAZON_BESTSELLERS_LINKS where Relevant_Date = '"+Date+"' order by Category",engine)

        urls = urls[urls['Category'].isin(Name_Matches["Amazon_Name"].unique())]['Link']
        urls = list(set(urls))

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse1,headers=header)


    def parse1(self, response):
        header = {'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}
        Urls = response.xpath('//div[@role="group"]/div/a/@href').extract()

        Urls1 = ["https://www.amazon.in"+x for x in Urls]
        Urls1 = list(set(Urls1))

        Urls1 = [x for x in Urls1 if x != response.url]
        if(len(Urls1) == 0):
            yield scrapy.Request(url=response.url, callback=self.parse_next,dont_filter = False,headers = header)

        else:
            yield scrapy.Request(url=response.url, callback=self.parse_next,dont_filter = False,headers = header)
            for url in Urls1:
                yield scrapy.Request(url=url, callback=self.parse2,dont_filter = False,headers = header)

    def parse2(self, response):
        header = {'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}
        Urls = response.xpath('//div[@role="group"]/div/a/@href').extract()

        Urls1 = ["https://www.amazon.in"+x for x in Urls]
        Urls1 = list(set(Urls1))

        Urls1 = [x for x in Urls1 if x != response.url]
        if(len(Urls1) == 0):
            yield scrapy.Request(url=response.url, callback=self.parse_next,dont_filter = False,headers = header)

        else:
            yield scrapy.Request(url=response.url, callback=self.parse_next,dont_filter = False,headers = header)
            for url in Urls1:
                yield scrapy.Request(url=url, callback=self.parse3,dont_filter = False,headers = header)

    def parse3(self, response):
        header = {'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}
        Urls = response.xpath('//div[@role="group"]/div/a/@href').extract()

        Urls1 = ["https://www.amazon.in"+x for x in Urls]
        Urls1 = list(set(Urls1))

        Urls1 = [x for x in Urls1 if x != response.url]
        if(len(Urls1) == 0):
            yield scrapy.Request(url=response.url, callback=self.parse_next,dont_filter = False,headers = header)

        else:
            yield scrapy.Request(url=response.url, callback=self.parse_next,dont_filter = False,headers = header)
            for url in Urls1:
                yield scrapy.Request(url=url, callback=self.parse4,dont_filter = False,headers = header)
    def parse4(self, response):
        header = {'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}
        Urls = response.xpath('//div[@role="group"]/div/a/@href').extract()

        Urls1 = ["https://www.amazon.in"+x for x in Urls]
        Urls1 = list(set(Urls1))

        Urls1 = [x for x in Urls1 if x != response.url]
        if(len(Urls1) == 0):
            yield scrapy.Request(url=response.url, callback=self.parse_next,dont_filter = False,headers = header)

        else:
            yield scrapy.Request(url=response.url, callback=self.parse_next,dont_filter = False,headers = header)
            for url in Urls1:
                yield scrapy.Request(url=url, callback=self.parse_next,dont_filter = False,headers = header)


    def parse_next(self, response):
        header = {'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}
        res =  response.body
        soup = BeautifulSoup(response.text, 'lxml') # ADD Rajesh 'lxml'
        if "Subscription API" in str(soup):
            print("######################################BLOCKING######################################")
        else:
            print("######################################NOT BLOCKING######################################")
        print(response)
        try:
          category = response.xpath("//div[@role='treeitem']/span/text()").extract()[0]
        except:
          category = response.xpath('//span[@class="_p13n-zg-nav-tree-all_style_zg-selected__1SfhQ"]/text()').extract()[0]
        sub_category_1 = ''
        sub_category_2 = ''
        sub_category_3 = ''
        sub_category_4 = ''

        #print(response.css('mytag ::text'))
        #print(response.xpath("//div[@class='_p13n-zg-nav-tree-all_style_zg-browse-group__88fbz']/text()").extract())
        print(self.dict)
        tags = response.xpath("//div[@class='_p13n-zg-nav-tree-all_style_zg-browse-group__88fbz']")
        combos1 = []
        for t in tags:
            text2 = t.xpath('.//text()').extract()
            combos1.append(text2)

        b = sum(combos1,[])
        b = [x for x in b if x!="‹\xa0"]
        if len(combos1)==5:
          combos = [b[0],b[1],b[2],b[3],category]
        elif len(combos1)==4:
          combos = [b[0],b[1],b[2],category]
        elif len(combos1)==3:
          combos = [b[0],b[1],category]
        elif len(combos1)==2:
          combos = [b[0],category]
        else:
          combos = [category]

        combos = list(OrderedDict.fromkeys(combos))

        if len(combos)==4:
            combos.append(None)
        elif len(combos)==3:
            combos.append(None)
            combos.append(None)
        elif len(combos)==2:
            combos.append(None)
            combos.append(None)
            combos.append(None)
        elif len(combos)==1:
            combos.append(None)
            combos.append(None)
            combos.append(None)
            combos.append(None)

        print("######",combos,"######")
        print(response.url)
        self.dict['Link'] = response.url
        self.dict['Category'] = combos[0]
        self.dict['Sub_Category_1'] = combos[1]
        self.dict['Sub_Category_2'] = combos[2]
        self.dict['Sub_Category_3'] = combos[3]
        self.dict['Sub_Category_4'] = combos[4]
        df = pd.DataFrame(self.dict,index=[0])
        table_name = Name_Matches[Name_Matches["Amazon_Name"]==combos[0]]['Table_Name'].iloc[0]      
        df['Table_Name'] = table_name        
        print(df)
        df['Relevant_Date'] = pd.to_datetime(today.strftime("%Y-%m-%d"))
        df['Runtime'] = pd.to_datetime(datetime.datetime.now(india_time).strftime("%Y-%m-%d %H:%M:%S"))

        print(table_name, df)

        df.to_sql(name="AMAZON_BESTSELLER_CATEGORY_LINKS",con=engine,if_exists='append',index=False)
        print("Uploaded ",df.shape)


        #del df
