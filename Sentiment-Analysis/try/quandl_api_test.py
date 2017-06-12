import quandl
import pandas

quandl.ApiConfig.api_key = 'umGymx6FEb-Bm2xRFRGV'

ref_data = quandl.get('TSE/1547', returns="numpy") # S&P 500
cor_data = quandl.get_table('WIKI/PRICES') # 3000 US companies

df_ref = pandas.DataFrame(data=ref_data)
df_ref['Ticker'] = '^GSPC'

df_ref.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'ticker']
df_ref = df_ref[['ticker', 'date', 'open', 'high', 'low', 'close', 'volume']]

df_cor = pandas.DataFrame(data=cor_data)
df_cor = df_cor.reindex(columns=['ticker', 'date', 'open', 'high', 'low', 'close', 'volume'])

df_all = df_cor.append(df_ref)
