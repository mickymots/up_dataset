from Ticker import Ticker
from datetime import datetime
import csv
from os import getenv
from apitools import getDateTuple

# build dataset for given number of days and the stock
def build_dataset(symbol, days, apiKey):
    ticker = Ticker(symbol, days, apiKey)
    return ticker.build_dataset()

# write the data to file
def write_to_file(data, file_name):
    csv_columns = data.keys()
    csv_out=csv.DictWriter(open(file_name, 'a'), fieldnames=csv_columns)
    csv_out.writeheader()
    csv_out.writerow(data)

#build output file name for the ticker
def build_file_name(ticker, day):
    file_version = (str)(datetime.today().timestamp())[:11]
    file_name = f'data/{ticker}_{file_version}.csv'
    return file_name


# build dataset for given number of days and the stock
def build_dataset(ticker, day, apiKey):
    # print(f'build dataset called for = {ticker}')
    ticker = Ticker(ticker, day, apiKey)
    return ticker.build_dataset()

def main(day_offset, ticker):
    day = getDateTuple(day_offset)
    print(f"{day} - {ticker}")
    try:
        get_dataset(ticker, day)
    except ValueError as error:
        print(f'{ticker} - {error}')

    

# main functions to build the dataset
def get_dataset(ticker, day):
    if(getenv('apiKey')):
        apiKey = getenv('apiKey')

        data = build_dataset(ticker=ticker, day=day, apiKey=apiKey)
        print(data)
        if data:
            write_to_file(data, file_name= build_file_name(ticker, day))
        else:
            raise ValueError(f'No record for {day}')

    else:
        raise EnvironmentError('set the apiKey env variable')


if __name__ == '__main__':
    try:
        day_offset = int(input('Enter of day to query for?  '))
        main(ticker, day_offset)
    except EnvironmentError as error:
        print(error)
    
