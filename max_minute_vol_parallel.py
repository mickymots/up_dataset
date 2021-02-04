from Ticker import Ticker
from datetime import datetime
import csv
from os import getenv

# build dataset for given number of days and the stock
def build_dataset(symbol, days, apiKey):
    ticker = Ticker(symbol, days, apiKey)
    return ticker.build_dataset()

# write the data to file
def write_to_file(data, file_name):
    csv_out=csv.writer(open(file_name, 'a'))
    for line in data:
        if line:
            csv_out.writerow(line)

#build output file name for the ticker
def build_file_name(ticker):
    file_version = (str)(datetime.today().timestamp())[:11]
    file_name = f'{ticker}_1min_max_volume_{file_version}.csv'
    return file_name

# main functions to build the dataset
def main():
    if(getenv('apiKey')):
        days = int(input('Enter # of days to query for? '))
        ticker = input('Enter ticker of the stock ? ')
        apiKey = getenv('apiKey')
        data = build_dataset(symbol=ticker, days=days, apiKey=apiKey)
        write_to_file(data, file_name= build_file_name(ticker))

    else:
        raise EnvironmentError('set the apiKey env variable')

if __name__ == '__main__':
    try:
        main()
    except EnvironmentError as error:
        print(error)
    
