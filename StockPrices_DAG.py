from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests

default_args = {
 'owner': 'jChong02',
 'email': ['junjie.chong@sjsu.com'],
 'retries': 1,
 'retry_delay': timedelta(minutes=3),
}

def return_snowflake_conn(con_id):

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id=con_id, warehouse='compute_wh', database='hw5', schema='raw_data')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

@task
def extract(url):
  try:
    response = requests.get(url)

    if response.status_code != 200:
      raise Exception(f'Request failed with status code {r.status_code}')

    data = response.json()

    if 'Error Message' in data:
        raise Exception(f"API Error: {data['Error Message']}")

    return data

  except requests.exceptions.RequestException as e:
    raise Exception(f"Request failed: {e}")


@task
def transform(raw_data, symbol):
  if 'Time Series (Daily)' not in raw_data:
    raise KeyError(f"Unexpected data format: 'Time Series (Daily)' key missing")

  results = [
        {
            'date': key,
            'open': value['1. open'],
            'high': value['2. high'],
            'low': value['3. low'],
            'close': value['4. close'],
            'volume': value['5. volume'],
            'symbol': symbol
        }
        for key, value in raw_data['Time Series (Daily)'].items()
    ]

  # Get only the last 90 days worth of data
  return results[:90] if len(results) >= 90 else results


@task
def load(data, con, target_table):
  try:
        con.execute("BEGIN;")

        con.execute(f"DROP TABLE IF EXISTS {target_table};")

        con.execute(f'''CREATE OR REPLACE TABLE {target_table} (
                          "date" DATE,
                          open FLOAT,
                          high FLOAT,
                          low FLOAT,
                          close FLOAT,
                          volume INTEGER,
                          symbol VARCHAR,

                          PRIMARY KEY("date", symbol)
                          );''')

        # Start loading the data into the table
        insert_query = f'''INSERT INTO {target_table}
                           ("date", open, high, low, close, volume, symbol)
                           VALUES (%s, %s, %s, %s, %s, %s, %s);'''

        for record in data:
            con.execute(insert_query, (
                record['date'],
                record['open'],
                record['high'],
                record['low'],
                record['close'],
                record['volume'],
                record['symbol']
            ))

        con.execute("COMMIT;")
        print("Data loaded successfully.")
  except Exception as e:
    con.execute("ROLLBACK;")
    print(f"An error occurred: {e}")
    raise e


with DAG(
    dag_id = 'StockPrices',
    start_date = datetime(2024,9,28),
    catchup=False,
    tags=['ETL'],
    default_args=default_args,
    schedule = '30 2 * * *'
) as dag:
    alpha_vantage_key = Variable.get("alpha_vantage_key")
    target_table = "hw5.raw_data.stock_data"
    symbol = "MSFT"
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={alpha_vantage_key}&datatype=json'
    cur = return_snowflake_conn("snowflake-conn")

    raw_data = extract(url)
    data = transform(raw_data, symbol)
    load(data, cur, target_table)