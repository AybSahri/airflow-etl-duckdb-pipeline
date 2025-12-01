import duckdb
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import logging


DB_PATH = "/opt/airflow/db/ecommerce_data.duckdb"



# -----------------------------
# Default arguments for DAG
# -----------------------------
default_args = {
    "owner": "ayoub",                 # Who “owns” this DAG
    "depends_on_past": False,         # Tasks don’t wait for previous runs
    "retries": 3,                     # Retry 3 times if a task fails
    "retry_delay": timedelta(minutes=5),  # Wait 5 min between retries
    "email_on_failure": False,        # Disable email for now
    "email_on_retry": False
}

def load_report_tables():
    con = duckdb.connect(DB_PATH)
    try:
        # -----------------------------
        # DAILY REVENUE
        # -----------------------------
        con.execute("""
            CREATE OR REPLACE TABLE report_layer.daily_revenue AS
            SELECT
                    d.date_id,
                    d.year,
                    d.month,
                    d.day,
                    COUNT(f.order_id) AS total_orders,
                    SUM(f.total_amount) AS revenue
            FROM refined_layer.fact_sales AS f
            JOIN refined_layer.dim_date AS d
            USING (date_id)
            GROUP BY d.date_id, d.year, d.month, d.day
                    """)

        # -----------------------------
        # REVENUE BY PRODUCT
        # -----------------------------
        con.execute("""
            CREATE OR REPLACE TABLE report_layer.revenue_by_product AS
            SELECT
                    p.product_id,
                    p.product_name,
                    COUNT(f.order_id) AS total_orders,
                    SUM (f.total_amount) AS revenue
            FROM refined_layer.fact_sales AS f
            JOIN refined_layer.dim_product AS p
            USING (product_id)
            GROUP BY p.product_id, p.product_name
                    """)

        # -----------------------------
        # REVENUE BY COUNTRY
        # -----------------------------
        con.execute("""
            CREATE OR REPLACE TABLE report_layer.revenue_by_country AS
            SELECT
                    c.country_code,
                    COUNT(f.order_id) AS total_orders,
                    SUM(f.total_amount) AS revenue
            FROM refined_layer.fact_sales AS f
            JOIN refined_layer.dim_country AS c
            USING(country_code)
            GROUP BY c.country_code
                    """)
        
        logging.info("Report layer tables updated successfully.")
    
    except Exception as e:
        logging.info(f"Failed to load report layer: {e}")
        raise

    finally:
        con.close()

with DAG(
    dag_id= "report_analysis",
    default_args=default_args,
    description="Load report layer tables from refined layer",
    start_date= datetime(2024, 1, 1),
    schedule= "@hourly",
    catchup= False
) as dag:
    
    transform_task= PythonOperator(
        task_id= "report_sales_data",
        python_callable= load_report_tables
    )