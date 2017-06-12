#!/usr/bin/python
import json
import os
import re
import sys

import pandas
import quandl

# output file name: input/stockPrices_raw.json
#          ticker
#         /  |   \       
#     open close adjust ...
#       /    |     \
#    dates dates  dates ...

quandl.ApiConfig.api_key = 'umGymx6FEb-Bm2xRFRGV'


def get_all():
    fin = open('./input/finished.reuters')

    ref_data = quandl.get('TSE/1547', returns="numpy")  # S&P 500
    cor_data = quandl.get_table('WIKI/PRICES')  # 3000 US companies

    df_ref = pandas.DataFrame(data=ref_data)
    df_ref['Ticker'] = '^GSPC'

    df_ref.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'ticker']
    df_ref = df_ref[['ticker', 'date', 'open', 'high', 'low', 'close', 'volume']]

    df_cor = pandas.DataFrame(data=cor_data)
    df_cor = df_cor.reindex(columns=['ticker', 'date', 'open', 'high', 'low', 'close', 'volume'])

    df = df_cor.append(df_ref)

    df.to_csv('all_stocks.csv', sep=',', encoding='utf-8')

    df.to_csv('^GSPC.csv', sep=',', encoding='utf-8').loc[df['ticker'] == '^GSPC']

    for num, line in enumerate(fin):
        ticker = line.strip()
        df.to_csv(ticker + '.csv', sep=',', encoding='utf-8').loc[df['ticker'] == ticker]



def calc_finished_ticker():
    os.system("awk -F',' '{print $1}' ./input/news_reuters.csv | sort | uniq > ./input/finished.reuters")

def get_stock_Prices():
    fin = open('./input/finished.reuters')
    output = './input/stockPrices_raw.json'

    # exit if the output already existed
    if os.path.isfile(output):
        sys.exit("Prices data already existed!")

    priceSet = {}
    # reference stock - IMPORTANT
    priceSet['^GSPC'] = PRICE('^GSPC')  # download S&P 500
    for num, line in enumerate(fin):
        ticker = line.strip()
        priceSet[ticker] = PRICE(ticker)
        # if num > 10: break # for testing purpose

    with open(output, 'w') as outfile:
        json.dump(priceSet, outfile, indent=4)

def PRICE(ticker):
    csv = open(ticker + '.csv').read().split('\n')

    ticker_price = {}
    index = ['open', 'high', 'low', 'close', 'volume']
    for num, line in enumerate(csv):
        line = line.strip().split(',')
        if len(line) < 7 or num == 0: continue
        date = line[0]
        # check if the date type matched with the standard type
        if not re.search(r'^[12]\d{3}-[01]\d-[0123]\d$', date): continue
        # open, high, low, close, volume, adjClose : 1,2,3,4,5,6
        for num, typeName in enumerate(index):
            try:
                ticker_price[typeName][date] = round(float(line[num + 1]), 2)
            except:
                ticker_price[typeName] = {}
    return ticker_price


if __name__ == "__main__":
    calc_finished_ticker()
    get_stock_Prices()
