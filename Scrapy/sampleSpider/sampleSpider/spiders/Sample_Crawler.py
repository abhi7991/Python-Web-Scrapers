# -*- coding: utf-8 -*-
"""
Created on Tue Oct 15 11:02:27 2024

@author: abhis
"""


'''

Simple Spider to collect data from multiple pages 
This uses multi-concurrency and can bee modified
in the settings file.

The flow of each spider can also be modified using
the pipeline code to direect the daata to a desired 
location

This spider consists of a single parse function
each parse function can be used to penetrate a layer
of the website

'''

import scrapy
from scrapy import Selector

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

#Custom function to clean up data from a webpage
def clean_up(response):
    all_url = response.xpath('//div[@class="image_container"]/a/@href').extract()
    all_title = response.xpath('//article[@class="product_pod"]/h3/a/text()').extract()
    rating = response.xpath('//article[@class="product_pod"]/p/@class').extract()
    price = response.xpath('//div[@class="product_price"]/p[@class="price_color"]/text()').extract()
    availability = response.xpath('//div[@class="product_price"]/p[@class="instock availability"]/text()').extract()
    availability = [re.sub(r'\s+', ' ', x).strip() for x in availability]
    availability = [x for x in availability if x!=''   ]
    rating = [x.split(" ")[-1] for x in rating]        
    data = {"Url":all_url,"Title":all_title, "Rating":rating,"Price":price,"Availability":availability}
    data = pd.DataFrame(data)
    section_name = response.xpath('//div[@class="page-header action"]/h1/text()').extract()
    section_name  = [x.strip() for x in section_name][0]        
    data['Section'] = section_name
    return data



#Main Spider Class
class CarSpider(scrapy.spiders.Spider):
    name = "sampleCrawler"
    
    #Starting point for the crawler
    def start_requests(self):
        header = {'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}    
        #Specifying a dictionary data structure
        self.dict = {}     
        url = 'https://books.toscrape.com/'
        yield scrapy.Request(url=url, callback=self.parse1,headers=header)
    
    '''
    This function collects all the books at a single level
    without the need for going into sub-levels/categories
    '''    
    def parse1(self, response): 
        url = 'https://books.toscrape.com/'
        header = {'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}            
        data = clean_up(response)
        print(data)

        '''

        The crawler needs to go into subsequent pages
        to get the data. I have capped thee numbeer of pages at 10

        '''
        no_of_pages = 10
        next_page_url = 'https://books.toscrape.com/'+response.xpath('//div/ul[@class="pager"]/li[@class="next"]/a/@href').extract()[0]
        if "catalogue" not in next_page_url:
            next_page_url = "https://books.toscrape.com/catalogue/"+response.xpath('//div/ul[@class="pager"]/li[@class="next"]/a/@href').extract()[0]
        current_page = response.xpath('//div/ul[@class="pager"]/li[@class="current"]/text()').extract()[0].strip().split(" ")[1]
        print(current_page," ",next_page_url)
        current_page = int(current_page.split("Page ")[0])
        
        if ((current_page<=no_of_pages)):
            yield scrapy.Request(url=next_page_url, callback=self.parse1,dont_filter = True,headers = header)
        else:
            print("We Done")
    
        
        
        