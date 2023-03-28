import yfinance as fin
import pandas as pd
from datetime import date, timedelta, time
from google.cloud import bigquery

t = fin.Tickers( 'msft aapl amzn tsla' )
#df = t.history("360d")
df = t.history("1d")
df1 = df.stack()

table_id ="cpoderoso-dtsc.stockMkt.stock"
client = bigquery.Client()

columns = list(df1)
coldb   = [ 'close_vl', 'dividends_vl', 'high_vl', 'low_vl', 'open_vl', 'stock_splits', 'volume_amt' ]
z       = 0 # to set Dataframe Symbol and Date values per Column
ist     = '['
for x in df1.index:
    ist =  ist + " { 'date_id': '"+ x[0].strftime('%Y-%m-%d') + "', 'symbol_nm': '" + x[1] + "'"
    w   = 0 # to set the DB Column Name
    for i in columns:
        ist = ist + ", '" + coldb[w] + "': " + str(df1[i][z])
        w   = w + 1
    ist = ist + " },"

    z = z + 1

rows_to_insert = eval( ist +' ]' )
# print( rows_to_insert )
errors = client.insert_rows_json(
    table_id,
    rows_to_insert
)

if errors ==[]:
    print("New rows have been added.")
else:
    print("Encountered errors while inserting rows: {}".format(errors))
