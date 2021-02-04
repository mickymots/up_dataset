from multiprocessing import Pool
from apitools import *


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
            return data
            
        
            