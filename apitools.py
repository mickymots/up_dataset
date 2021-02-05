from urllib import request
from datetime import datetime, timedelta, date
import csv
from functools import reduce
import json
import requests

BASE_URL = 'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{timespan}/{multiplier}/{start}/{end}?unadjusted=true&sort=asc&limit=50000&apiKey={apiKey}'
TRADE_API = 'https://api.polygon.io/v2/ticks/stocks/trades/{ticker}/{start}?limit=50000&apiKey={apiKey}&reverse=false'

def call_vol_api(start, end, timespan, mutliplier, ticker, limit, apiKey):
    url = BASE_URL.format(ticker=ticker, multiplier=mutliplier, timespan=timespan, start=start, end=end, apiKey=apiKey )
    with requests.get(url) as response:
        return json.loads(response.text)

def call_trades_api(start, ticker, apiKey, timestamp):
    # timestamp=1602682745425209383&
    url = TRADE_API.format(ticker=ticker, start=start, apiKey=apiKey )
    if(timestamp):
        url = url + f'&timestamp={timestamp}'

    with requests.get(url) as response:
        return json.loads(response.text)

def getStartTimeRange(date_range):
    start_base = datetime(2021, 1, 31)
    for i in range(date_range):
        start_day = start_base - timedelta(days=i)
        yield start_day.strftime("%Y-%m-%d")

def getTimeRange(date_range):
    start_base = datetime(2021, 1, 31, 9,0,0) 
    end_base = datetime(2021, 1, 31, 23,59, 59)

    for i in range(date_range):
        start_day = start_base - timedelta(days=i)
        end_day = end_base - timedelta(days=i)

        start = start_day.strftime("%Y-%m-%d")
        start_ts = (int)(datetime.timestamp(start_day) * 1000)
        end_ts = (int)(datetime.timestamp(end_day) * 1000)
        
        yield (start, start_ts, end_ts)

def get_max_minute_volume(data):
    if('results' in data):
        results = data['results']
        max_volume = reduce(reducer_fn, results)
        return max_volume

# combined_data = {'v': 145761325.0, 'vw': 139.1824, 'o': 139.52, 'c': 137.09, 'h': 141.99, 'l': 136.7, 't': 1611810000000, 'v_1min_max': 3319830.0}

def record_generator(data, ticker):
    timestamp = datetime.fromtimestamp((int)(data['t']/1000))
    record = (timestamp.strftime('%Y%m%d'), ticker,  data['v'], data['vw'], data['0'], data['c'], data['h'] , data['l'], data['v_1min_max'])
    return record

def reducer_fn(item1, item2):
    return item1 if item1['v'] >=  item2['v'] else item2