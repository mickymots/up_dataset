from multiprocessing import Pool
from apitools import *

class Ticker:
    
    limit = 50000

    def __init__(self, ticker, days, apiKey):
        self.ticker = ticker
        self.days = days
        self.apiKey = apiKey


    def get_volume_record_for_minute(self, day):
        (start, start_ts, end_ts) = day
        per_min_volume_data = call_vol_api(start_ts,end_ts, 1, 'minute',self.ticker, 50000, self.apiKey)
        max_1_min_volume = get_max_minute_volume(per_min_volume_data)

        if max_1_min_volume:
            return {'v_1min_max': max_1_min_volume['v'] }
        else:
            return  {'v_1min_max': 0}


    def get_volume_record_for_day(self, day):
        (start, start_ts, end_ts)  = day
        self.get_trades_record_for_day(day)
        # per_min_volume_data = self.get_volume_record_for_minute(day)


        # per_day_volume_data = call_vol_api(start_ts, end_ts, 1, 'day',self.ticker, 50000, self.apiKey)
        
        # if per_day_volume_data['resultsCount']:
        #     return {**per_day_volume_data['results'][0] , **per_min_volume_data}
        
    
    def get_trades_record_for_day(self, day):

        (start, start_ts, end_ts)  = day
        # print(f"day = {start}")

        more_pages = True
        timestamp = None
        count = 0
        while more_pages:
            data = call_trades_api(start, self.ticker, apiKey=self.apiKey, timestamp=timestamp)
            
            if('results_count' in data):
                results_count = data['results_count']
                more_pages = Ticker.limit <= results_count

                if(results_count):
                    count += 1
                    last_record = data['results'][-1]
                    timestamp = last_record['t']
            else:
                more_pages = False
        
        print(f'total pages = {count} for {start}')
        return count

    def build_dataset(self):
        # with Pool(8) as p:
        #     data = list(filter(lambda v: v , p.map(self.get_volume_record_for_day, getTimeRange(self.days))))
        #     return data
        
        return [self.get_volume_record_for_day(day) for day in getTimeRange(self.days)]
           
            
        
            