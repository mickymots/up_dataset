from urllib import request
from datetime import datetime, timedelta, date
import csv
from functools import reduce
import json
import requests

BASE_URL = 'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{timespan}/{multiplier}/{start}/{end}?unadjusted=true&sort=asc&limit=50000&apiKey={apiKey}'

def call_api(start, end, timespan, mutliplier, ticker, limit, apiKey):
    url = BASE_URL.format(ticker=ticker, multiplier=mutliplier, timespan=timespan, start=start, end=end, apiKey=apiKey )
    with requests.get(url) as response:
        return json.loads(response.text)


def getTimeRange(date_range):
    start_base = datetime(2021, 1, 31) 
    end_base = datetime(2021, 1, 31, 23,59, 59)

    for i in range(date_range):
        start_base_day = start_base - timedelta(days=i)
        end_base_day = end_base - timedelta(days=i)

        start = (int)(datetime.timestamp(start_base_day) * 1000)
        end = (int)(datetime.timestamp(end_base_day) * 1000)
        
        yield (start,end)

def get_max_minute_volume(data):
    if('results' in data):
        results = data['results']
        max_volume = reduce(reducer_fn, results)
        return max_volume

def record_generator(data, ticker):
    timestamp = datetime.fromtimestamp((int)(data['t']/1000))
    record = (timestamp.strftime('%Y%m%d'), ticker,  data['v'])
    return record

def reducer_fn(item1, item2):
    return item1 if item1['v'] >=  item2['v'] else item2