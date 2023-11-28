import pandas as pd
from datetime import datetime, timedelta
import requests
import csv
from io import StringIO
import logging

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
dag_weather = DAG(
    '2_weather_etl_dag',
    default_args=default_args,
    description='Weather ETL DAG',
    schedule_interval='@daily',
)

# Define your extraction function
def extract_data(**kwargs):
    logging.info("Extract task is starting")

    try:
        # Get the current year and current date
        current_year = datetime.now().year
        current_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        # Create the URL with dynamic startDateTime and endDateTime values
        url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/weatherdata/history?&aggregateHours=24&startDateTime={current_year}-01-01T00:00:00&endDateTime={current_date}&unitGroup=metric&contentType=csv&dayStartTime=0:0:00&dayEndTime=0:0:00&location=US&key=9KVZ52XFSR3SZVFPKWAXWB3AS"

        # Make the request to the API
        response = requests.get(url)

        # Check if the request was successful
        response.raise_for_status()

        # Extract data from the API response
        csv_data = response.text

        # Read data CSV from string
        df = pd.read_csv(StringIO(csv_data))

        # Dynamically generate the file path based on DAG run execution date and task instance
        execution_date = kwargs['execution_date']
        task_instance_str = kwargs['ti'].task_id
        output_file_path = f'/opt/airflow/data/weather_{execution_date}_{task_instance_str}.csv'

        # Save data CSV to a file
        df.to_csv(output_file_path, index=False)

        # Push the dynamically generated file path to XCom
        kwargs['ti'].xcom_push(key='extracted_file_path', value=output_file_path)

        logging.info("Extract task is done")

    except requests.RequestException as e:
        logging.error(f"Error in API request: {e}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        raise

# Define your transformation function
def transform_data(**kwargs):
    logging.info("Transform task is starting")

    # Retrieve the file path from the XCom pushed by the extract task
    input_file_path = kwargs['ti'].xcom_pull(task_ids='extract_data', key='extracted_file_path')

    # Read data from the extracted CSV file
    data = pd.read_csv(input_file_path)

    # Transformations
    data['Date time'] = pd.to_datetime(data['Date time'], format='%m/%d/%Y') 
    data['Date time'] = pd.to_datetime(data['Date time'])

    # Dropping Columns that are not needed
    data = data.drop(['Address',
                      'Minimum Temperature', 
                      'Maximum Temperature',
                      'Latitude',
                      'Longitude',
                      'Resolved Address',
                      'Name',
                      'Info',
                      'Snow Depth',
                      'Visibility',
                      'Sea Level Pressure',
                      'Weather Type',
                      'Wind Gust', 
                      'Wind Chill', 
                      'Wind Direction',
                      'Heat Index',
                      'Precipitation Cover'], axis=1)

    # Dynamically generate the file path based on DAG run execution date and task instance
    execution_date = kwargs['execution_date']
    task_instance_str = kwargs['ti'].task_id
    output_file_path = f'/opt/airflow/data/transformed_weather_{execution_date}_{task_instance_str}.csv'

    # Save the transformed data to a new CSV file
    data.to_csv(output_file_path, index=False)

    # Push the dynamically generated file path to XCom
    kwargs['ti'].xcom_push(key='transformed_file_path', value=output_file_path)

    logging.info("Transform task is done")

# Define your load function
def load_data_into_postgres(**kwargs):
    logging.info("Load task is starting")

    # Retrieve the file path from the XCom pushed by the transform task
    input_file_path = kwargs['ti'].xcom_pull(task_ids='transform_data', key='transformed_file_path')

    # Read data from the transformed CSV file
    data = pd.read_csv(input_file_path)

    # Connect to the PostgreSQL database using SQLAlchemy
    engine = create_engine('postgresql://lvqpozlu:odGCNoUaSQWgj4AJg7J124dKsgnICxlA@rosie.db.elephantsql.com:5432/lvqpozlu')

    # Create a SQLAlchemy session
    Session = sessionmaker(bind=engine)
    session = Session()

    # Define the table outside the if-else block
    Base = declarative_base()

    class WeatherData(Base):
        __tablename__ = 'weather_data'

        id = Column(Float, primary_key=True, autoincrement=True)
        datetime = Column(DateTime, nullable=False)
        temperature = Column(Float)
        dew_point = Column(Float)
        relative_humidity = Column(Float)
        wind_speed = Column(Float)
        precipitation = Column(Float)
        cloud_cover = Column(Float)
        conditions = Column(String(255))

    # Check if the table exists
    inspector = inspect(engine)
    if not inspector.has_table('weather_data'):
        # If the table does not exist, create it
        Base.metadata.create_all(engine)
    else:
        # If the table exists, delete all rows
        session.query(WeatherData).delete()

    try:
        # Insert data into the 'weather_data' table
        for index, row in data.iterrows():
            weather_data = WeatherData(
                datetime=row['Date time'],
                temperature=row['Temperature'],
                dew_point=row['Dew Point'],
                relative_humidity=row['Relative Humidity'],
                wind_speed=row['Wind Speed'],
                precipitation=row['Precipitation'],
                cloud_cover=row['Cloud Cover'],
                conditions=row['Conditions'],
            )
            session.add(weather_data)

        # Commit the changes
        session.commit()

        logging.info("Load task is done")

    except Exception as e:
        logging.error(f"An error occurred while loading data into PostgreSQL: {e}")
        # Rollback the changes in case of an error
        session.rollback()
        raise

    finally:
        # Close the session
        session.close()

# Define the extract task
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag_weather,
)

# Define the transform task
transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag_weather,
)

# Define the load task
load_task = PythonOperator(
    task_id='load_data_into_postgres',
    python_callable=load_data_into_postgres,
    provide_context=True,
    dag=dag_weather,
)

# Set up task dependencies
extract_task >> transform_task >> load_task