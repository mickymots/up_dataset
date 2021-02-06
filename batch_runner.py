import csv
import pandas as pd
from dataset_builder import main as builder

from concurrent.futures import ThreadPoolExecutor
from functools import partial
from time import time
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_batch(day, batch):

    days = [i for i in range(700)]

    with ThreadPoolExecutor(max_workers=40) as executor:
        fn = partial(builder, 'ACI')
        executor.map(fn, days)



def main():
    ts = time()
    csv_df = pd.read_csv('../source/tickers.csv', header=0, usecols=['symbol'], chunksize=200, iterator=True)

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



        run_batch(4, batch)
    logging.info('Took %s seconds', time() - ts)   



if __name__ == '__main__':
    main()