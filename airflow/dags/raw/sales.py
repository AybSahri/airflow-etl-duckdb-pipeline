from airflow import DAG
from airflow.operators.python import PythonOperator # pyright: ignore[reportMissingImports]
from datetime import datetime
import requests # pyright: ignore[reportMissingModuleSource]
import duckdb # pyright: ignore[reportMissingImports]
import pandas as pd
from io import StringIO
import logging

API_URL = "https://my.api.mockaroo.com/sales"
API_KEY = "9d2036e0"
DUCKDB_FILE = "/opt/airflow/db/ecommerce_data.duckdb" 

# --------------------------

# Logging setup

# --------------------------

logging.basicConfig(level=logging.INFO,
format='%(asctime)s - %(levelname)s - %(message)s')

# --------------------------

# Functions

# --------------------------

def fetch_csv_from_api(**kwargs):
    """Fetch CSV data from Mockaroo API and push to XCom"""
    headers = {"X-API-Key": API_KEY}
    logging.info(f"Fetching CSV from URL: {API_URL}")
    response = requests.get(API_URL, headers=headers)
    logging.info(f"Response code: {response.status_code}")
    if response.status_code != 200:
        raise Exception(f"API request failed: {response.status_code}")
    
    csv_text = response.text
    logging.info("Fetched CSV from API successfully")
    
    # Push CSV to XCom
    kwargs['ti'].xcom_push(key='csv_data', value=csv_text)

def load_to_duckdb(**kwargs):
    """Load CSV from XCom into DuckDB"""
    ti = kwargs['ti']
    csv_text = ti.xcom_pull(key='csv_data', task_ids='fetch_api_task')
    
    df = pd.read_csv(StringIO(csv_text))
    df['order_date'] = pd.to_datetime(df['order_date'], dayfirst=False, errors='coerce')
    con = duckdb.connect(DUCKDB_FILE)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw_layer")
    con.execute("CREATE SCHEMA IF NOT EXISTS refined_layer")
    con.register("df_view", df)
    con.execute("""
                INSERT INTO raw_layer.sales (
                order_id, order_date, quantity, payment_method,
                order_status, customer_id, customer_name, email,
                gender, country_code, product_id, product_name, unit_price
                )
                SELECT * FROM df_view
                """)
    

    con.close()
    logging.info("Inserted data into raw_layer.sales successfully")


# --------------------------

# Airflow DAG

# --------------------------

with DAG(
    dag_id="load_sales_api_to_duckdb",
    start_date=datetime(2024, 1, 1),
    schedule="* * * * *",
    catchup=False
) as dag:
    
    fetch_api_task = PythonOperator(
        task_id="fetch_api_task",
        python_callable=fetch_csv_from_api
    )
    
    load_duckdb_task = PythonOperator(
        task_id="load_duckdb_task",
        python_callable=load_to_duckdb
    )

fetch_api_task >> load_duckdb_task
