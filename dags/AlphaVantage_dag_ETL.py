import pandas as pd
from datetime import datetime, timedelta
import requests
import logging
from io import StringIO

from sqlalchemy import create_engine, Column, Float, String, DateTime, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Define default_args
default_args = {
    'owner': 'airflow',
    'start_date': datetime.utcnow(),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate DAG
dag_commodities = DAG(
    '1_commodities_etl_dag',
    default_args=default_args,
    description='Commodities ETL DAG',
    schedule_interval='@daily',
)

# Define your extraction function for commodities data
def extract_commodities_data(**kwargs):
    logging.info("Extract task for commodities data is starting")

    try:
        # Extract data from API response
        url = "https://www.alphavantage.co/query?function=ALL_COMMODITIES&interval=monthly&apikey=69FRR5Y479JKUCVX"
        response = requests.get(url)
        json_data = response.json()
        df = pd.json_normalize(json_data['data'])

        # Dynamically generate the file path based on DAG run execution date and task instance
        execution_date = kwargs['execution_date']
        task_instance_str = kwargs['ti'].task_id
        output_file_path = f'/opt/airflow/data/commodities_{execution_date}_{task_instance_str}.csv'

        # Save data to a file
        df.to_csv(output_file_path, index=False)

        # Push the dynamically generated file path to XCom
        kwargs['ti'].xcom_push(key='extracted_file_path', value=output_file_path)

        logging.info("Extract task for commodities data is done")

    except requests.RequestException as e:
        logging.error(f"Error in API request for commodities data: {e}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred for commodities data: {e}")
        raise

# Define your transformation function for commodities data
def transform_commodities_data(**kwargs):
    logging.info("Transform task for commodities data is starting")

    # Retrieve the file path from the XCom pushed by the extract task
    input_file_path = kwargs['ti'].xcom_pull(task_ids='extract_commodities_data', key='extracted_file_path')

    # Read data from the extracted CSV file
    data = pd.read_csv(input_file_path)

    # Transformations
    current_year = datetime.now().year
    data['date'] = pd.to_datetime(data['date'])
    data = data[data['date'].dt.year == current_year]
    data = data.sort_values(by='date', ascending=True, ignore_index=True)

    # Dynamically generate the file path based on DAG run execution date and task instance
    execution_date = kwargs['execution_date']
    task_instance_str = kwargs['ti'].task_id
    output_file_path = f'/opt/airflow/data/transformed_commodities_{execution_date}_{task_instance_str}.csv'

    # Save the transformed data to a new CSV file
    data.to_csv(output_file_path, index=False)

    # Push the dynamically generated file path to XCom
    kwargs['ti'].xcom_push(key='transformed_file_path', value=output_file_path)

    logging.info("Transform task for commodities data is done")

# Define your load function for commodities data
def load_commodities_data_into_postgres(**kwargs):
    logging.info("Load task for commodities data is starting")

    # Retrieve the file path from the XCom pushed by the transform task
    input_file_path = kwargs['ti'].xcom_pull(task_ids='transform_commodities_data', key='transformed_file_path')

    # Read data from the transformed CSV file
    data = pd.read_csv(input_file_path)

    # Connect to the PostgreSQL database using SQLAlchemy
    engine = create_engine('postgresql://lvqpozlu:odGCNoUaSQWgj4AJg7J124dKsgnICxlA@rosie.db.elephantsql.com:5432/lvqpozlu')

    # Create a SQLAlchemy session
    Session = sessionmaker(bind=engine)
    session = Session()

    # Define the table outside the if-else block
    Base = declarative_base()

    class CommoditiesData(Base):
        __tablename__ = 'commodities_data'

        id = Column(Float, primary_key=True, autoincrement=True)
        date = Column(DateTime, nullable=False)
        value = Column(Float)  # Update this line based on your data structure
        # Add other columns based on your data structure

    # Check if the table exists
    inspector = inspect(engine)
    if not inspector.has_table('commodities_data'):
        # If the table does not exist, create it
        Base.metadata.create_all(engine)
    else:
        # Jika tabel sudah ada, Anda dapat memutuskan untuk menghapus data lama atau memperbarui data yang sudah ada.
        # Contoh: Hapus semua data dari tabel
        session.query(CommoditiesData).delete()

    try:
        # Insert data into the 'commodities_data' table
        for index, row in data.iterrows():
            new_data = CommoditiesData(date=row['date'], value=row['value'])  # Update this line based on your data structure
            session.add(new_data)

        # Commit the changes
        session.commit()

        logging.info("Load task for commodities data is done")

    except Exception as e:
        logging.error(f"An error occurred while loading data into PostgreSQL: {e}")
        # Rollback the changes in case of an error
        session.rollback()
        raise

    finally:
        # Close the session
        session.close()

# Define the extract task for commodities data
extract_commodities_task = PythonOperator(
    task_id='extract_commodities_data',
    python_callable=extract_commodities_data,
    provide_context=True,
    dag=dag_commodities,
)

# Define the transform task for commodities data
transform_commodities_task = PythonOperator(
    task_id='transform_commodities_data',
    python_callable=transform_commodities_data,
    provide_context=True,
    dag=dag_commodities,
)

# Define the load task for commodities data
load_commodities_task = PythonOperator(
    task_id='load_commodities_data_into_postgres',
    python_callable=load_commodities_data_into_postgres,
    provide_context=True,
    dag=dag_commodities,
)

# Set up task dependencies for the commodities data DAG
extract_commodities_task >> transform_commodities_task >> load_commodities_task