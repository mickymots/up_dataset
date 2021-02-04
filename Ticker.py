from urllib import request
from datetime import datetime, timedelta, date
import csv
from functools import reduce
import json
import requests
from multiprocessing import Pool
from max_minute_vol import getTimeRange, BASE_URL, call_api, record_generator, reducer_fn, get_max_minute_volume

class Ticker:
    
    def __init__(self, ticker, days, apiKey):
        self.ticker = ticker
        self.days = days
        self.apiKey = apiKey


    def get_max_for_day(self, day):
        (start, end)  = day
        per_min_volume_data = call_api(start,end, 1, 'minute',self.ticker, 50000, self.apiKey)
        max_volume = get_max_minute_volume(per_min_volume_data)
        if max_volume:
            return record_generator(data=max_volume, ticker=self.ticker)

    def build_dataset(self):
        with Pool(4) as p:
            data = p.map(self.get_max_for_day, getTimeRange(self.days))
            file_name = f'{self.ticker}_1min_max_volume_test.csv'
            csv_out=csv.writer(open(file_name, 'a'))
            for line in data:
                if line:
                    csv_out.writerow(line)
        
