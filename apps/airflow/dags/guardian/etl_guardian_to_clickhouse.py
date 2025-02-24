import logging.config
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import requests
import json
import os
import uuid
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

def update_etl_timestamp():
    """Update the timestamp for the ETL run."""
    Variable.set("etl_guardians_last_run", datetime.now().isoformat())

def extract_guardians_from_postgres():
    """Extract guardian data from PostgreSQL."""
    last_run_timestamp = Variable.get("etl_guardians_last_run", default_var="1970-01-01T00:00:00")
    postgres_hook = PostgresHook(postgres_conn_id='academic-local')
    
    # Adjusted SQL to get all guardians, ignoring the 'updatedAt' condition
    sql = f'''
        SELECT DISTINCT ON ("guardianId") 
        "guardianId", "schoolId", "firstName", "lastName", "firstNameNative", 
        "lastNameNative", "gender", "dob", "phone", "email", "address", 
        "photo", "createdAt", "updatedAt", "archiveStatus", "userName"
        FROM guardian
        ORDER BY "guardianId", "updatedAt" DESC;
    '''
    
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(data, columns=columns)

    # Convert timestamps for JSON serialization
    for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
        df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S')

    cursor.close()
    connection.close()
    
    # Log the data before returning it
    logging.info(f"Extracted guardian data: {df.head()}")

    if df.empty:
        logging.warning("No data found, sending empty response.")
    else:
        # Send the data even if it's old or unchanged
        logging.info(f"Data extracted: {df.shape[0]} rows")
    
    return df.to_dict('records')

def transform_guardians_data(**kwargs):
    """Transform the extracted guardian data."""
    data = kwargs['ti'].xcom_pull(task_ids='extract_guardians_from_postgres')

    # Log the extracted data
    logging.info(f"Data to transform: {data}")

    def format_value(value):
        """Format values based on type."""
        if value is None:
            return 'NULL'
        elif isinstance(value, str):
            try:
                uuid_obj = uuid.UUID(value)  # Check if value is a valid UUID
                return f"'{uuid_obj}'"  # Ensure UUID is properly quoted
            except ValueError:
                return f"'{value.replace("'", "''")}'"  # Escape single quotes in strings
        elif isinstance(value, (int, float)):
            return str(value)  # Keep numbers as they are
        else:
            return f"'{value}'"  # Default case for unknown types

    # Apply the transformation on each row
    transformed_rows = []
    for row in data:
        transformed_values = [format_value(value) for value in row.values()]
        transformed_rows.append(f"({','.join(transformed_values)})")

    # Log the transformed rows before returning them
    logging.info(f"Transformed data: {transformed_rows}")
    return transformed_rows

def load_guardians_to_clickhouse(**kwargs):
    """Load transformed guardian data into ClickHouse."""
    # Pull transformed data from XCom
    transformed_data = kwargs['ti'].xcom_pull(task_ids='transform_guardians_data')

    # Log the transformed data for debugging
    logging.info(f"Transformed data to insert: {transformed_data}")

    # Validate the data: Ensure it's not empty
    if not transformed_data:
        raise Exception("No data found to load into ClickHouse.")

    # Prepare the ClickHouse URL and query
    clickhouse_url = f'{os.getenv("CLICKHOUSE_HOST")}:{os.getenv("CLICKHOUSE_PORT")}'
    query = f'''
        INSERT INTO {os.getenv("CLICKHOUSE_DB")}.guardian 
        ("guardianId", "schoolId", "firstName", "lastName", "firstNameNative", 
        "lastNameNative", "gender", "dob", "phone", "email", "address", 
        "photo", "createdAt", "updatedAt", "archiveStatus", "userName")
        VALUES {','.join(transformed_data)}
    '''

    # Log the query before making the request (for debugging purposes)
    logging.info(f"ClickHouse Insert Query: {query}")

    # Send the request to ClickHouse
    response = requests.post(
        url=clickhouse_url,
        data=query,
        headers={'Content-Type': 'text/plain'},
        auth=(os.getenv("CLICKHOUSE_USER"), os.getenv("CLICKHOUSE_PASSWORD"))
    )

    # Log the response from ClickHouse
    logging.info(f"Response from ClickHouse: {response.status_code} - {response.text}")

    # Check if the request was successful
    if response.status_code != 200:
        raise Exception(f"Failed to load data to ClickHouse: {response.text}")

    # Return a success message with the number of rows loaded
    return f"Successfully loaded {len(transformed_data)} rows to ClickHouse"

# Define the DAG
dag = DAG(
    'guardians_to_clickhouse',
    default_args=default_args,
    description='Copy guardian data from Academic Service Postgres to ClickHouse',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['academic', 'guardian']
)

# Define tasks
extract_task = PythonOperator(
    task_id='extract_guardians_from_postgres',
    python_callable=extract_guardians_from_postgres,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_guardians_data',
    python_callable=transform_guardians_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_guardians_to_clickhouse',
    python_callable=load_guardians_to_clickhouse,
    provide_context=True,
    dag=dag,
)

update_timestamp = PythonOperator(
    task_id='update_etl_timestamp',
    python_callable=update_etl_timestamp,
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> load_task >> update_timestamp
