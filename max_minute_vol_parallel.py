from Ticker import Ticker
from datetime import datetime
import csv
from os import getenv
import pandas as pd
from multiprocessing import Pool
# build dataset for given number of days and the stock
def build_dataset(symbol='ANB', days=10, apiKey=getenv('apiKey')):
    ticker = Ticker(symbol, days, apiKey)
    data = ticker.build_dataset()
    return data

# write the data to file
def write_to_file(data, file_name):
    csv_out=csv.writer(open(file_name, 'a'))
    
    for line in data:
        csv_out.writerow(line.values())

#build output file name for the ticker
def build_file_name(ticker):
    file_version = (str)(datetime.today().timestamp())[:11]
    file_name = f'{ticker}_1min_max_volume_{file_version}.csv'
    return file_name

def group_by_char(ch):
    def group_fn(val):
        return True if val[0] == ch else False
    return group_fn

# main functions to build the dataset
def main():
    if(getenv('apiKey')):
        apiKey=getenv('apiKey')
        days = 10
        ticker = input('Enter ticker of the stock ? ')
        apiKey = getenv('apiKey')

        # df = pd.read_csv('./tickers.csv', header=0, usecols=['symbol'])    

       

        data = build_dataset(symbol=ticker, days=days, apiKey=apiKey)
        if(data):
            write_to_file(data, file_name= build_file_name(ticker))
        else:
            raise ValueError('No data to write')

    else:
        raise EnvironmentError('set the apiKey env variable')

def printx(val):
    
    build_dataset(symbol=ticker, days=days,apiKey=key)


if __name__ == '__main__':

    try:
        main()
    except ValueError as error:
        print(error)
    except EnvironmentError as error:
        print(error)
    
