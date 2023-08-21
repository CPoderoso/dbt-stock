def personalFinance():
    import yfinance as fin
    import pandas as pd
    from datetime import date, timedelta, time
    from google.cloud import bigquery

    t = fin.Tickers( 'msft aapl amzn tsla' )
    df = t.history("1d")
    df1 = df.stack()

    #print(df1)

    table_id ="<your Bigquery project and dataset>.stock"
    client = bigquery.Client()

    columns = list(df1)
    coldb   = [ 'close_vl', 'dividends_vl', 'high_vl', 'low_vl', 'open_vl', 'stock_splits', 'volume_amt' ]
    z       = 0 # to set Dataframe Symbol and Date values per Column
    ist     = '['
    for x in df1.index:
        ist =  ist + " { 'date_id': '"+ x[0].strftime('%Y-%m-%d') + "', 'symbol_nm': '" + x[1] + "'"
        w   = 0 # to set the DB Column Name
        for i in columns:
            if i != 'Capital Gains':
                #print( w, z, i)
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

################################

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, time
from pytz import timezone

# Define the Eastern Time (ET) time zone
eastern_tz = timezone('US/Eastern')

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 9),
    'end_date': None,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

# Convert 5:00 PM ET to UTC
# Eastern Time is UTC - 4 hours
target_hour = 16  # 4:15 PM
target_minute = 15
utc_time = time(target_hour, target_minute)

# Create the DAG
dag = DAG(
    dag_id='publicFinance_dag',
    default_args=default_args,
    description='Airflow DAG to run Personal Finance using Yahoo data',
    schedule=f'{utc_time.minute} {utc_time.hour} * * 1-5',  # Set the frequency of DAG execution
)

# Define the PythonOperator that runs your Python script
run_python_script = PythonOperator(
    task_id='run_python_script',
    python_callable=personalFinance,
    dag=dag,
)

# Set the task dependency
run_python_script
