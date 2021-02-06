import csv
import pandas as pd
from dataset_builder import main as builder

from concurrent.futures import ThreadPoolExecutor
from functools import partial
from time import time

from datetime import datetime, timedelta
import os
import logging
import numpy as numpy

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
headers = ['date','1mVolume','Ticker','Average Order','Median Order','LO_Size','LO_Exchange','LO_Condition','lo_per_vol','v','vw','o','c','h','l','t','High Break']
def run_batch(days, batch):

    days_list = [i for i in range(days)]

    with ThreadPoolExecutor(max_workers=40) as executor:
        fn = partial(builder, 'ACN')
        executor.map(fn, days_list)



def main():
    ts = time()
    csv_df = pd.read_csv('./source/tickers.csv', header=0, usecols=['symbol'], chunksize=200, iterator=True)

    i = 0
    j = 1

    # reading in batch/chunks of 10 tickers
    for df in csv_df:

        df = df.rename(columns={c: c.replace(' ', '') for c in df.columns}) 
        df.index += j
        i+=1
        batch = []
        for ind in df.index:
            batch.append(df['symbol'][ind])



        run_batch(730, batch)
    logging.info('Took %s seconds', time() - ts)   
    
    ts_batch_start = time()
    process_batch()    
    logging.info('Batch Processing Took %s seconds', time() - ts_batch_start)   

def process_batch():
    ts_batch_start = time()
    
    csv_df = pd.read_csv('/home/amit/projects/python-workspace/upwork-dataset/data/dataset_2yrs_ACN_060221.csv', header=0)
    
    csv_df.sort_values(by='t', inplace=True, ascending=False)
    csv_df.fillna(0)
    numpy_list = csv_df.to_numpy()

    calcualte_breakouts(numpy_list)
    
    pd.DataFrame(numpy_list).to_csv("boo.csv", header=headers, index=None)
    logging.info('Batch Processing Took %s seconds', time() - ts_batch_start)  
    
     
    print(numpy_list)

def calcualte_breakouts(numpy_list):
    i = 0
    
    for item in numpy_list:
        los = item[5]
        fromDt = getPDDate(item[0])
        foundDt = getPDDate(item[0])

        try:
            foundDt = next(getPDDate(x[0]) for x in numpy_list[i+1:] if x[5] >= los)
        except Exception:
            foundDt = getPDDate(numpy_list[-1][0])
            i+1
            
        
        days = numpy.busday_count(foundDt, fromDt)
        item.put(-1,(days))
        
        i += 1
    
def getPDDate(value):
    return pd.to_datetime(value,format="%Y-%m-%d").date()
    
if __name__ == '__main__':
    process_batch()