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


def calc_finished_ticker():
    os.system("awk -F',' '{print $1}' ./input/news_reuters.csv | sort | uniq > ./input/finished.reuters")

def get_all():
    fin = open('./input/finished.reuters')

    if not os.path.isfile('./stocks/^GSPC.csv'):
        # S&P 500 should be from ^GSPC
        ref_data = quandl.get('TSE/1547', returns="numpy")  # S&P 500
        cor_data = quandl.get_table('WIKI/PRICES')  # 3000 US companies

        df_ref = pandas.DataFrame(data=ref_data)
        df_ref['Ticker'] = '^GSPC'

        df_ref.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'ticker']
        df_ref = df_ref[['ticker', 'date', 'open', 'high', 'low', 'close', 'volume']]
        df_ref['adjclose'] = 0

        df_cor = pandas.DataFrame(data=cor_data)
        df_cor = df_cor.reindex(columns=['ticker', 'date', 'open', 'high', 'low', 'close', 'volume', 'adjclose'])

        df = df_cor.append(df_ref)

        sp500 = df.loc[df['ticker'] == '^GSPC']
        sp500 = sp500.reindex(columns=['date', 'open', 'high', 'low', 'close', 'volume', 'adjclose'])
        sp500.set_index('date', inplace=True)
        sp500.to_csv('./stocks/^GSPC.csv', sep=',', encoding='utf-8', header=False)

        for num, line in enumerate(fin):
            ticker = line.strip()
            ticker_data = df.loc[df['ticker'] == ticker]
            ticker_data = ticker_data.reindex(columns=['date', 'open', 'high', 'low', 'close', 'volume', 'adjclose'])
            ticker_data.set_index('date', inplace=True)
            ticker_data.to_csv('./stocks/' + ticker + '.csv', sep=',', encoding='utf-8', header=False)

        df.set_index('date', inplace=True)
        df.to_csv('./stocks/all_stocks.csv', sep=',', encoding='utf-8')


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
    csv = open('./stocks/' + ticker + '.csv').read().split('\n')
    ticker_price = {}
    index = ['open', 'high', 'low', 'close', 'volume', 'adjclose']
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
    get_all()
    get_stock_Prices()
