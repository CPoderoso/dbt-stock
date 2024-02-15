def personalFinanceYahoo():
    import yfinance as fin
    import pandas as pd
    from datetime import date, timedelta, time
    from google.cloud import bigquery

    t = fin.Tickers( 'msft aapl amzn tsla' )
    #df = t.history( start='2023-07-31', end='2023-08-05')
    df = t.history("1d")
    df1 = df.stack()

    #print(df1)

    table_id ="<your_bigqyeryProject>.stock"
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

def personalFinanceTransactions():
    import os
    import json
    from google.cloud import bigquery


    # Specify the path to the folder containing your JSON files
    json_folder_path = "/mnt/c/Users/chpod/Google Drive/appsheet/data/PersonalYFinance-8046497/Files/"

    # Initialize an empty dictionary to store the merged data
    merged_data = []

    # Iterate through each JSON file in the folder
    for filename in os.listdir(json_folder_path):
        if filename.endswith('.json'):
            json_file_path = os.path.join(json_folder_path, filename)
            with open(json_file_path, 'r') as json_file:
                data = json.load(json_file)
                merged_data.append(data)
    if merged_data != []:
        table_id ="cpoderoso-dtsc.dbt_cpoderoso.transactions"
        client = bigquery.Client()

        # print( merged_data )

        errors = client.insert_rows_json(
            table_id,
            merged_data
            )

        if errors ==[]:
            # Will delete JSON files only if the insert succeed
            for filename in os.listdir(json_folder_path):
                if filename.endswith('.json'):
                    json_file_path = os.path.join(json_folder_path, filename)
                    os.remove(json_file_path)
            print("New rows have been added and CDC files deleted.")   
        else:
            print("Encountered errors while inserting rows: {}".format(errors))
    else:
        print('No Transactions to add today')

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
    'retries': 2,
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
run_Yahoo = PythonOperator(
    task_id='run_Yahoo',
    python_callable=personalFinanceYahoo,
    dag=dag,
)

run_Transactions = PythonOperator(
    task_id='run_Transactions',
    python_callable=personalFinanceTransactions,
    trigger_rule='all_success',
    dag=dag,
)

# Set the task dependency
run_Yahoo >> run_Transactions

