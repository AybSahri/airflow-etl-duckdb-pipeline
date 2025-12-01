import duckdb  # pyright: ignore[reportMissingImports]
from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator  # pyright: ignore[reportMissingImports]
import logging

DB_PATH = "/opt/airflow/db/ecommerce_data.duckdb"

def load_refined_tables():
    con = duckdb.connect(DB_PATH)
    try:
        # -----------------------------
        # DIM CUSTOMER
        # -----------------------------
        con.execute("""
            INSERT OR IGNORE INTO refined_layer.dim_customer
            SELECT DISTINCT
                customer_id,
                REGEXP_REPLACE(LOWER(customer_name), '(^|\\s)([a-z])', '\\1\\U\\2') AS customer_name,
                LOWER(email) AS email,
                CASE
                    WHEN LOWER(gender) IN ('f', 'female') THEN 'Female'
                    WHEN LOWER(gender) IN ('m', 'male') THEN 'Male'
                    ELSE 'Unknown'
                END AS gender,
                UPPER(country_code) AS country_code
            FROM raw_layer.sales
            WHERE customer_id IS NOT NULL;
        """)

        # -----------------------------
        # DIM PRODUCT
        # -----------------------------
        con.execute("""
            INSERT OR IGNORE INTO refined_layer.dim_product
            SELECT DISTINCT
                product_id,
                REGEXP_REPLACE(LOWER(product_name), '(^|\\s)([a-z])', '\\1\\U\\2') AS product_name,
                COALESCE(unit_price, 0) AS unit_price
            FROM raw_layer.sales
            WHERE product_id IS NOT NULL;
        """)

        # -----------------------------
        # DIM DATE
        # -----------------------------
        con.execute("""
            INSERT OR IGNORE INTO refined_layer.dim_date
            SELECT DISTINCT
                order_date AS date_id,
                EXTRACT(YEAR FROM order_date)::INTEGER AS year,
                EXTRACT(MONTH FROM order_date)::INTEGER AS month,
                EXTRACT(DAY FROM order_date)::INTEGER AS day
            FROM raw_layer.sales
            WHERE order_date IS NOT NULL;
        """)

        # -----------------------------
        # DIM PAYMENT METHOD
        # -----------------------------
        con.execute("""
            INSERT OR IGNORE INTO refined_layer.dim_payment_method (payment_method)
            SELECT DISTINCT
                    COALESCE (payment_method, 'Unknown') AS payment_method
            FROM raw_layer.sales;
        """)

        # -----------------------------
        # DIM COUNTRY
        # -----------------------------
        con.execute("""
            INSERT OR IGNORE INTO refined_layer.dim_country (country_code)
            SELECT DISTINCT country_code
            FROM raw_layer.sales
            WHERE country_code IS NOT NULL;
        """)

        # -----------------------------
        # FACT TABLE (UPSERT)
        # -----------------------------
        
        # Remove old rows for the same order_id
        con.execute("""
            DELETE FROM refined_layer.fact_sales
            WHERE order_id IN (
                SELECT DISTINCT order_id FROM raw_layer.sales
            );
        """)

        # Insert fresh rows
        con.execute("""
            INSERT INTO refined_layer.fact_sales
            SELECT
                order_id,
                order_date AS date_id,
                customer_id,
                country_code,
                product_id,
                quantity,
                COALESCE(quantity * unit_price, 0) AS total_amount,
                COALESCE(payment_method, 'Unknown') AS payment_method,
                REGEXP_REPLACE(LOWER(order_status), '(^|\\s)([a-z])', '\\1\\U\\2') AS order_status,
                ingestion_timestamp
            FROM raw_layer.sales
            WHERE order_id IS NOT NULL;
        """)

        logging.info("Refined layer tables updated successfully.")

    except Exception as e:
        logging.error(f"Failed to load refined tables: {e}")
        raise

    finally:
        con.close()

# -----------------------------
# DAG
# -----------------------------
with DAG(
    dag_id="transform_raw_to_refined",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
) as dag:

    transform_task = PythonOperator(
        task_id="transform_sales_data",
        python_callable=load_refined_tables
    )
