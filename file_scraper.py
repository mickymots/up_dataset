import datetime
from urllib import request
encoding = 'utf-8'

def open_short_api(date):
    url = f'http://regsho.finra.org/CNMSshvol{date}.txt'
    try:
        with request.urlopen(url) as response:
            data = response.read()
            return data

    except:
        print(f'date = {date}')

def getDate(date_range):
    base = datetime.datetime.today() - datetime.timedelta(days=5)
    for i in range(date_range):
        day = base - datetime.timedelta(days=i)
        year = str(day.year)
        month = str(day.month).rjust(2, '0')
        date = str(day.day).rjust(2, '0')
        yield year+month+date



for date in getDate(400):
    short_data =  open_short_api(date)

    if short_data:
        short_data_str = short_data.decode(encoding)
        sink = open(f'short{date}.txt', 'a')
        sink.write(short_data_str)
