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