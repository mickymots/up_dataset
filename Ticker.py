from multiprocessing import Pool
from apitools import *


class Ticker:
    
    def __init__(self, ticker, days, apiKey):
        self.ticker = ticker
        self.days = days
        self.apiKey = apiKey


    def get_record_for_min(self, day):
        (start, end)  = day
        per_min_volume_data = call_api(start,end, 1, 'minute',self.ticker, 50000, self.apiKey)
        max_1_min_volume = get_max_minute_volume(per_min_volume_data)

        if max_1_min_volume:
            return {'v_1min_max': max_1_min_volume['v'] }
        else:
            return  {'v_1min_max': 0}


    def get_record_for_day(self, day):
        (start, end)  = day
        per_min_volume_data = self.get_record_for_min(day)

        per_day_volume_data = call_api(start,end, 1, 'day',self.ticker, 50000, self.apiKey)
        
        if per_day_volume_data['resultsCount']:
            return {**per_day_volume_data['results'][0] , **per_min_volume_data}
        
        
    def build_dataset(self):
        with Pool(4) as p:
            data = list(filter(lambda v: v , p.map(self.get_record_for_day, getTimeRange(self.days))))
            return data
            
        
            