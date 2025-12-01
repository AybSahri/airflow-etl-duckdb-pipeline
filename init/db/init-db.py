import os
import duckdb  # type: ignore
import logging

# -------------------------
# 1. Configure Logging
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# -------------------------
# 2. Build the correct DB path
# -------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  
DB_FILE = os.path.abspath(os.path.join(
    BASE_DIR, "..", "..", "airflow", "db", "ecommerce_data.duckdb"
))

# -------------------------
# 3. Connect to DuckDB
# -------------------------
try:
    con = duckdb.connect(DB_FILE)
    logging.info(f"Connected to DuckDB database: {DB_FILE}")
except Exception as e:
    logging.error(f"Failed to connect to DuckDB: {e}")
    raise


# -------------------------
# 4. Init function → Create schemas + tables
# -------------------------
def init_sales_db(con):

    try:
        # Create schemas
        con.execute("CREATE SCHEMA IF NOT EXISTS raw_layer")
        con.execute("CREATE SCHEMA IF NOT EXISTS refined_layer")
        con.execute("CREATE SCHEMA IF NOT EXISTS report_layer")

        logging.info("Schemas created successfully")

        # ---------------------
        # RAW TABLE
        # ---------------------
        con.execute("DROP TABLE IF EXISTS refined_layer.fact_sales")
        con.execute("""
            CREATE TABLE IF NOT EXISTS raw_layer.sales (
                order_id INTEGER,
                order_date DATE,
                quantity INTEGER,
                payment_method VARCHAR(50),
                order_status VARCHAR(50),
                customer_id INTEGER,
                customer_name VARCHAR(255),
                email VARCHAR(50),
                gender VARCHAR(20),
                country_code VARCHAR(10),
                product_id INTEGER,
                product_name VARCHAR(255),
                unit_price DECIMAL(10, 2),
                ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        logging.info("Created raw_layer.sales table")

        # ---------------------
        # FACT TABLE
        # ---------------------
        con.execute("""
            CREATE TABLE IF NOT EXISTS refined_layer.fact_sales (
                order_id INTEGER,
                date_id DATE,
                customer_id INTEGER,
                country_code VARCHAR(10),
                product_id INTEGER,
                quantity INTEGER,
                total_amount DECIMAL(12,2),
                payment_method VARCHAR(50),
                order_status VARCHAR(50),
                ingestion_timestamp TIMESTAMP
            );
        """)
        logging.info("Created refined_layer.fact_sales table")

        # ---------------------
        # DIM CUSTOMER
        # ---------------------
        con.execute("""
            CREATE TABLE IF NOT EXISTS refined_layer.dim_customer (
                customer_id INTEGER PRIMARY KEY,
                customer_name VARCHAR,
                email VARCHAR,
                gender VARCHAR,
                country_code VARCHAR
            );
        """)
        logging.info("Created dim_customer table")

        # ---------------------
        # DIM PRODUCT
        # ---------------------
        con.execute("""
            CREATE TABLE IF NOT EXISTS refined_layer.dim_product (
                product_id INTEGER PRIMARY KEY,
                product_name VARCHAR,
                unit_price DECIMAL(10,2)
            );
        """)
        logging.info("Created dim_product table")

        # ---------------------
        # DIM DATE
        # ---------------------
        con.execute("""
            CREATE TABLE IF NOT EXISTS refined_layer.dim_date (
                date_id DATE PRIMARY KEY,
                year INTEGER,
                month INTEGER,
                day INTEGER
            );
        """)
        logging.info("Created dim_date table")

        # ---------------------
        # DIM COUNTRY
        # ---------------------
        con.execute("""
            CREATE TABLE IF NOT EXISTS refined_layer.dim_country (
                country_code VARCHAR PRIMARY KEY
            );
        """)
        logging.info("Created dim_country table")

        # ---------------------
        # DIM PAYMENT METHOD
        # ---------------------
        con.execute("""
            CREATE TABLE IF NOT EXISTS refined_layer.dim_payment_method (
                payment_method VARCHAR PRIMARY KEY
            );
        """)
        logging.info("Created dim_payment_method table")

        logging.info("Database connection closed")

    except Exception as e:
        logging.error(f"Error initializing DB: {e}")
        raise

# -------------------------
# 5. Run initialization
# -------------------------
init_sales_db(con)
con.close()
logging.info("Database connection closed")