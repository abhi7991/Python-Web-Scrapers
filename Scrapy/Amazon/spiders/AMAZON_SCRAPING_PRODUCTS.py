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

import re
import ast
import datetime as datetime
from pytz import timezone
import requests, zipfile, io
import csv
import numpy as np


os.chdir('C:/Adqvest')  # 'C:/Users/Administrator/Documents'

#os.chdir('C:/Users/Administrator/AdQvestDir')  # 'C:/Users/Administrator/Documents'
#os.environ["http_proxy"] = "http://localhost:12345"

#DB Connection

conndetail = pd.read_csv('C:/Adqvest/Amazon_AdQvest_properties.txt',delim_whitespace=True)

hostdet = conndetail.loc[conndetail['Env'] == 'Host']
port = conndetail.loc[conndetail['Env'] == 'port']
DBname = conndetail.loc[conndetail['Env'] == 'DBname']
host = list(hostdet.iloc[:,1])
port = list(port.iloc[:,1])
dbname = list(DBname.iloc[:,1])
Connectionstring = 'mysql+pymysql://' + host[0] + ':' + port[0] + '/' + dbname[0]
engine = sqlalchemy.create_engine(Connectionstring)

connection = engine.connect()

india_time = timezone('Asia/Kolkata')
today = datetime.datetime.now(india_time)
yesterday = datetime.datetime.now(india_time) - datetime.timedelta(1)




table_name = 'AMAZON_GROCERY_AND_GOURMET_FOODS'




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

def check_link(x2):
    x1='https://www.amazon.in/gp/bestsellers/grocery/ref=zg_bs_nav_0/257-4348904-1141216',    
    if x1==x2:
        raise Exception("##################SAME LINK#######################")
    else:
        print("ALL GOOD")
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

class CarSpider(scrapy.spiders.Spider):
    name = "amazon_newV3"


    def start_requests(self):
        header = {'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}
        self.dict = {}
        self.dict['Name'] = []
        self.dict['Price'] = []
        self.dict['Rank'] = []
        self.dict['Is_Prime'] = [] # Add - Rajesh
        self.dict['Category'] = []
        self.dict['Sub_Category_1'] = []
        self.dict['Sub_Category_2'] = []
        self.dict['Sub_Category_3'] = []
        self.dict['Sub_Category_4'] = []

        query = "select max(Relevant_Date) as RD from AdqvestDB.AMAZON_BESTSELLERS_LINKS"
        Date = pd.read_sql(query, con=engine).iloc[0,0].strftime("%Y-%m-%d")
        #Date = "'"+today.strftime("%Y-%m-%d")+"'"
        urls = pd.read_sql("Select * from AMAZON_BESTSELLERS_LINKS where Relevant_Date = '"+Date+"' order by Category",engine)
        urls = list(urls[urls['Category']=='Grocery & Gourmet Foods']['Link'])        


        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse1,headers=header)


    def parse1(self, response):
        header = {'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}
        Urls = response.xpath('//div[@role="group"]/div/a/@href').extract()

        Urls1 = ["https://www.amazon.in"+x for x in Urls]
        print("URLSZZZZZZZZZZZZZ1 : ", len(Urls1))
        if(len(Urls1) ==0):
            yield scrapy.Request(url=response.url, callback=self.parse_next,dont_filter = True,headers = header)
        else:
            for url in Urls1:
                print("URL",url)
                check_link(url)
                yield scrapy.Request(url=url, callback=self.parse2,dont_filter = True,headers = header)

    def parse2(self, response):
        header = {'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}
        Urls = response.xpath('//div[@role="group"]/div/a/@href').extract()
        Urls2 = ["https://www.amazon.in"+x for x in Urls]        
        print("URLSZZZZZZZZZZZZZ_2 : ", len(Urls2))        
        if(len(Urls2) == 0):
            yield scrapy.Request(url=response.url, callback=self.parse_next,dont_filter = True,headers = header)
        else:
        #yield scrapy.Request(url=response.url, callback=self.parse_next,dont_filter = True,headers = header)
            for url in Urls2:
                print("URL",url)       
                check_link(url)                 
                yield scrapy.Request(url=url, callback=self.parse3,dont_filter = True,headers = header)

    def parse3(self, response):
        header = {'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}
        Urls = response.xpath('//div[@role="group"]/div/a/@href').extract()
        Urls3 = ["https://www.amazon.in"+x for x in Urls]        
        print("URLSZZZZZZZZZZZZZ_3 : ", len(Urls3))        
        if(len(Urls3) == 0):
            yield scrapy.Request(url=response.url, callback=self.parse_next,dont_filter = True,headers = header)

        else:
            #yield scrapy.Request(url=response.url, callback=self.parse_next,dont_filter = True,headers = header)
            for url in Urls3:
                print("URL",url)
                check_link(url)                            
                yield scrapy.Request(url=url, callback=self.parse4,dont_filter = True,headers = header)

    def parse4(self, response):
        header = {'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}
        Urls = response.xpath('//div[@role="group"]/div/a/@href').extract()
        Urls4 = ["https://www.amazon.in"+x for x in Urls]        
        print("URLSZZZZZZZZZZZZZ_4 : ", len(Urls4))        
        if(len(Urls4) == 0):
            yield scrapy.Request(url=response.url, callback=self.parse_next,dont_filter = True,headers = header)
 
        else:
        #yield scrapy.Request(url=response.url, callback=self.parse_next,dont_filter = True,headers = header)
            for url in Urls4:
                print("URL",url)           
                check_link(url)                 
                yield scrapy.Request(url=url, callback=self.parse_next,dont_filter = True,headers = header)


    def parse_next(self, response):
        header = {'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}
        res =  response.body
        if(response.xpath('//div[@role = "treeitem"]/span/@href/text()').extract() != []):

            category = response.xpath('//div[@role = "treeitem"]/span/@href/text()').extract()[0]
            sub_category_1 = response.xpath('//div[@role = "treeitem"]/a/text()').extract()[1]
            sub_category_2 = response.xpath('//div[@role = "treeitem"]/a/text()').extract()[2]
            sub_category_3 = response.xpath('//div[@role = "treeitem"]/a/text()').extract()[3]
            sub_category_4 = response.xpath('//div[@role = "treeitem"]/a/text()').extract()[0]

        # elif(response.xpath('//ul[@id = "zg_browseRoot"]/ul/ul/ul/ul/li/span/text()').extract() != []):

        #     category = response.xpath('//ul[@id = "zg_browseRoot"]/ul/li/a/text()').extract()[0]
        #     sub_category_1 = response.xpath('//ul[@id = "zg_browseRoot"]/ul/ul/li/a/text()').extract()[0]
        #     sub_category_2 = response.xpath('//ul[@id = "zg_browseRoot"]/ul/ul/ul/li/a/text()').extract()[0]
        #     sub_category_3 = response.xpath('//ul[@id = "zg_browseRoot"]/ul/ul/ul/ul/li/span/text()').extract()[0]
        #     sub_category_4 = ''

        # elif(response.xpath('//ul[@id = "zg_browseRoot"]/ul/ul/ul/li/span/text()').extract() != []):

        #     category = response.xpath('//ul[@id = "zg_browseRoot"]/ul/li/a/text()').extract()[0]
        #     sub_category_1 = response.xpath('//ul[@id = "zg_browseRoot"]/ul/ul/li/a/text()').extract()[0]
        #     sub_category_2 = response.xpath('//ul[@id = "zg_browseRoot"]/ul/ul/ul/li/span/text()').extract()[0]
        #     sub_category_3 = ''
        #     sub_category_4 = ''


        # elif(response.xpath('//ul[@id = "zg_browseRoot"]/ul/ul/li/span/text()').extract() != []):

        #     category = response.xpath('//ul[@id = "zg_browseRoot"]/ul/li/a/text()').extract()[0]
        #     sub_category_1 = response.xpath('//ul[@id = "zg_browseRoot"]/ul/ul/li/span/text()').extract()[0]
        #     sub_category_2 = ''
        #     sub_category_3 = ''
        #     sub_category_4 = ''

        else:
            try:
                category = response.xpath('//span[@class="_p13n-zg-nav-tree-all_style_zg-selected__1SfhQ"]/text()').extract()[0]
                sub_category_1 = ''
                sub_category_2 = ''
                sub_category_3 = ''
                sub_category_4 = ''

            except:
                print("I AM HERE ")
                print(df)
                #yield scrapy.Request(url=response.url, callback=self.parse_next,dont_filter = True,headers = header)


        #rank = Selector(text=res).xpath('//span[@class="a-size-small aok-float-left zg-badge-body zg-badge-color"]//text()').extract()
        rank = Selector(text=res).xpath('//span[@class="zg-bdg-text"]//text()').extract()        
        #name = Selector(text=res).xpath('//div[@class="a-section a-spacing-small"]/img/@alt').extract()
        name = Selector(text=res).xpath('//div[@class="a-section a-spacing-mini _p13n-zg-list-grid-desktop_maskStyle_noop__3Xbw5"]/img/@alt').extract()        
        #next_page = Selector(text=res).xpath('//li[@class="a-last"]/a/@href').extract()
        #print(rank,name)
        soup = BeautifulSoup(response.text, 'lxml') # ADD Rajesh 'lxml'
        if "Subscription API" in str(soup):
            print("######################################BLOCKING######################################")
        else:
            print("######################################NOT BLOCKING######################################")
        #data = soup.findAll('span',class_='aok-inline-block zg-item')
        data = soup.findAll('div',class_='zg-grid-general-faceout')        

        price = []
        Is_Prime = [] # Add - Rajesh

        for row in data:
            try:
                p_list = []
                #price_soup_list = row.findAll('span',class_='p13n-sc-price')
                price_soup_list = row.findAll('span',class_='_p13n-zg-list-grid-desktop_price_p13n-sc-price__3mJ9Z')                
                for p_row in price_soup_list:
                    p_val = p_row.text.replace(',','')
                    p_val = re.findall(r'-?\d+\.?\d*',p_val)[0]
                    p_list.append(p_val)
                price.append(p_list)
            except AttributeError:
                price.append([np.nan])
                pass
            except IndexError:
                price.append([np.nan])
                pass
            # Add - Rajesh
            if row.findAll('i', class_='a-icon a-icon-prime a-icon-small') != []:
                Is_Prime.append('Prime')
            else:
                Is_Prime.append('Not Prime')
            # Add - Rajesh

        for i in range(len(rank)):

            self.dict['Name'] = name
            self.dict['Category'] = category
            self.dict['Sub_Category_1'] = sub_category_1
            self.dict['Sub_Category_2'] = sub_category_2
            self.dict['Sub_Category_3'] = sub_category_3
            self.dict['Sub_Category_4'] = sub_category_4
            self.dict['Rank'] = rank
            self.dict['Price'] = price
            self.dict['Is_Prime'] = Is_Prime # Add - Rajesh

        #print(self.dict)

        df = pd.DataFrame(self.dict)
        print(df)

        # df['Price'] = df['Price'].map(str).str.replace(',','')
        # df['Price'] = df['Price'].map(str).apply(lambda x: re.findall(r'-?\d+\.?\d*',x))

        df['Min_Price'] = df['Price'].map(Min_Price)
        df['Max_Price'] = df['Price'].map(Max_Price)
        # df['Price'] = np.where(df['Min_Price'] == df['Max_Price'],df['Min_Price'],np.nan)
        df['Price'] = df['Max_Price']

        df['Brand'] = df['Name'].str.split(' ')
        df['Brand'] = df['Brand'].map(f)

        # df['SKU'] = df['Name'].map(str).apply(lambda x: re.findall(r'-?\d+\.?\d*',x))
        # df['SKU'] = df['SKU'].map(f)
        # df['SKU'] = np.where(~df['Category'].isin(['Amazon Launchpad','Baby Products','Beauty','Grocery & Gourmet Foods']),'',df['SKU'])
        # df['SKU'] = df['SKU'].apply(lambda x: str(x).replace('-',''))
        df = extractPiecesAndSku(df)

        # df = df.apply(lambda x: str(x)) # ADD Rajesh
        df['Price'] = df['Price'].apply(lambda x: str(x)) # ADD Rajesh
        df = df.apply(lambda x: x.str.strip())

        query = "select * from AdqvestDB.AMAZON_BESTSELLER_INDEX"
        dq = pd.read_sql(query, con=engine)
        df = df.merge(dq, how='left',on='Category')
        df['Category_Index'] = df['Category_Index'].fillna(45)
        df['QTY'] = df['QTY'].astype(float)



        df['Relevant_Date'] = pd.to_datetime(today.strftime("%Y-%m-%d"))
        df['Runtime'] = pd.to_datetime(datetime.datetime.now(india_time).strftime("%Y-%m-%d %H:%M:%S"))

        # print(df)
        df.to_sql(name=table_name+"_Temp_Abhi",con=engine,if_exists='append',index=False)
        print("Uploaded")
        #f.to_csv(r'C:/Adqvest/Scrapy/temp_amazon.csv', index=False)
        del df

        # if len(next_page) != 0:
        #     yield scrapy.Request(url=next_page[0], callback=self.parse_next,dont_filter = True,headers = header)
