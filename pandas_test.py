import pandas as pd
import json
from time import time

with open('./2021-01-27.json', 'r') as file:

    ts = time()

    data = json.load(file)

    df = pd.json_normalize(data['results'])
    print(df)

    sum = df['s'].sum()
    max = df['s'].max()
    maxIdx = df['s'].idxmax()
    print(df.iloc[41697])
    print(df.iloc[41697]['c'])
    print(df.iloc[41697]['i'])
    print(f'sum {sum}, max = {max}')

    print(f'{time()-ts} time ')