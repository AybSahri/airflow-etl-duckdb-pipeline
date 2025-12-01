# **ETL Pipeline with Airflow, Docker, and DuckDB** #


An end-to-end data engineering project that demonstrates how to build a modern ETL pipeline using Apache Airflow, Docker, Python, and DuckDB. The pipeline ingests data from an API, stores it in raw and refined layers, and generates reporting outputs — all orchestrated inside Docker.


## **🚀 Project Overview** ##

This project is a practical, production-style ETL pipeline designed to showcase key data engineering skills:
- Containerized orchestration using Airflow running in Docker
- API ingestion with Python
- Storage and transformation using DuckDB (local OLAP database)
- Multi-layer data architecture (raw → refined → reports)
- Logging, error handling, and reproducible workflows
- Volume management using Docker Compose

This project can be used as a portfolio piece for Data Engineering roles.

## **🧱 Architecture** ##

                +-------------------+
                |    API Source     |
                +---------+---------+
                          |
                          v
                +-------------------+
                |   Airflow DAG     |
                |    (Dockerized)   |
                +---------+---------+
                          |
                +---------v---------+
                | Raw Layer (DuckDB)|
                +---------+---------+
                          |
                +---------v---------+
                |  Refined Layer    |
                | (Transformations) |
                +---------+---------+
                          |
                +---------v---------+
                |  Reporting Layer  |
                +-------------------+

## **🛠️ Tech Stack** ##
- Apache Airflow (Scheduler & Orchestrator)
- Docker / Docker Compose
- Python (ETL scripts)
- DuckDB (analytical storage)
- Requests / Pandas
- Logging & Error Handling

## **⚙️ How to Run the Project** ##

  1. Clone the repository
  
    git clone https://github.com/YOUR_USERNAME/YOUR_REPO.git
    cd YOUR_REPO

  3. Start Airflow and all services
  
    docker-compose up --build

  4. Access Airflow UI

    http://localhost:8080

  5. Default Airflow Credentials
     
    username: airflow

    password: airflow

  6. Trigger the ETL DAG

    Go to Airflow UI     
     
    Find the DAG     
     
    Turn it ON and run it manually (or wait for the schedule)     

## **📊 Outputs** ##
- Depending on the transformations performed, the project generates:
- Raw data tables
- Cleaned & transformed tables in refined layer
- Aggregated reports stored in DuckDB or exported as CSV

## **🔮 Future Improvements** ##
- Add Great Expectations for data validation
- Add dbt for the transformation layer
- Integrate cloud storage (S3, GCS)
- Add unit tests (pytest)
- Add CI/CD (GitHub Actions)

## **👨‍💻 Author** ##

**Ayoub Sahri**
- Data Engineer / Data Analyst / Digital Business Analyst
- Passionate about building scalable, real-world data pipelines


If you find this project useful, feel free to ⭐ the repository!
