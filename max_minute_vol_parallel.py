from urllib import request
from datetime import datetime, timedelta, date
import csv
from functools import reduce
import json
import requests
from multiprocessing import Pool
from Ticker import Ticker
encoding = 'utf-8'


if __name__ == '__main__':
    days = int(input('Enter # of days to query for? '))
    symbol = input('Enter ticker of the stock ? ')
    apiKey = input('Enter API Key: ')
    ticker = Ticker(symbol, days, apiKey)
    ticker.build_dataset()
   
