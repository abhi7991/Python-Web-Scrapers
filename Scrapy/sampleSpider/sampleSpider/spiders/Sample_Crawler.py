# -*- coding: utf-8 -*-
"""
Created on Tue Oct 15 11:02:27 2024

@author: abhis
"""

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


class CarSpider(scrapy.spiders.Spider):
    name = "sampleCrawler"
    
    
    def start_requests(self):
        header = {'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}    
        self.dict = {}     
        url = 'https://books.toscrape.com/'
        yield scrapy.Request(url=url, callback=self.parse1,headers=header)
        
    def parse1(self, response): 
        url = 'https://books.toscrape.com/'
        header = {'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}            
        data = clean_up(response)
        print(data)
        next_page_url = 'https://books.toscrape.com/'+response.xpath('//div/ul[@class="pager"]/li[@class="next"]/a/@href').extract()[0]
        if "catalogue" not in next_page_url:
            next_page_url = "https://books.toscrape.com/catalogue/"+response.xpath('//div/ul[@class="pager"]/li[@class="next"]/a/@href').extract()[0]
        current_page = response.xpath('//div/ul[@class="pager"]/li[@class="current"]/text()').extract()[0].strip().split(" ")[1]
        print(current_page," ",next_page_url)
        current_page = int(current_page.split("Page ")[0])
        
        if ((current_page<=10)):
            yield scrapy.Request(url=next_page_url, callback=self.parse1,dont_filter = True,headers = header)
        else:
            print("We Done")
    
        
        
        