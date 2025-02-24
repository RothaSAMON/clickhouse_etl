import logging.config
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from sshtunnel import SSHTunnelForwarder
import psycopg2
from datetime import datetime
import pandas as pd
import requests
import json
import logging
import os
from dotenv import load_dotenv
import uuid

# Load environment variables from the .env file
load_dotenv()

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

def update_etl_timestamp():
    Variable.set("etl_teachers_last_run", datetime.now().isoformat())

def extract_teachers_from_postgres():
    """Extract data from PostgreSQL."""
    # Fetch the last ETL run timestamp (default: earliest date if not set)
    last_run_timestamp = Variable.get("etl_teachers_last_run", default_var="1970-01-01T00:00:00")

    # The postgres_conn_id refers to the connection ID configured in Airflow, which contains the necessary credentials to connect to your PostgreSQL database.
    postgres_hook = PostgresHook(postgres_conn_id='academic-local')  # PostgreSQL connection ID
    sql = f'''
        SELECT DISTINCT ON ("teacherId") 
        "teacherId", "schoolId", "campusId", "groupStructureId", "structureRecordId", 
        "subjectId", "employeeId", "firstName", "lastName", "firstNameNative", 
        "lastNameNative", "idCard", "gender", "email", "phone", 
        "position", "createdAt", "updatedAt", "department", "archiveStatus"
        FROM teacher
        WHERE "updatedAt" > '{last_run_timestamp}'
        ORDER BY "teacherId", "updatedAt" DESC;
    '''
    
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(data, columns=columns)

    # Convert timestamps to string format for JSON serialization
    for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
        df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S')

    cursor.close()
    connection.close()
    return df.to_dict('records')


def load_teachers_to_clickhouse(**kwargs):
    """Load data into ClickHouse with proper string escaping."""
    data = kwargs['ti'].xcom_pull(task_ids='extract_teachers_from_postgres')

    def format_value(value):
        """Format values with proper type handling."""
        if value is None:
            return 'NULL'
        elif isinstance(value, str):
            # Try to determine if the string is a UUID
            try:
                uuid.UUID(value)
                return f"toUUID('{value}')"
            except ValueError:
                # Escape single quotes for regular strings
                escaped_value = value.replace("'", "\\'")
                return f"'{escaped_value}'"
        else:
            return str(value)

    # Format rows with proper type handling
    formatted_rows = []
    for row in data:
        formatted_values = [format_value(value) for value in row.values()]
        formatted_rows.append(f"({','.join(formatted_values)})")

    # Prepare the ClickHouse HTTP endpoint and query
    clickhouse_url = f'{os.getenv("CLICKHOUSE_HOST")}:{os.getenv("CLICKHOUSE_PORT")}'
    query = f'''
            INSERT INTO {os.getenv("CLICKHOUSE_DB")}.teacher 
            ("teacherId", "schoolId", "campusId", "groupStructureId", "structureRecordId", 
            "subjectId", "employeeId", "firstName", "lastName", "firstNameNative", 
            "lastNameNative", "idCard", "gender", "email", "phone", 
            "position", "createdAt", "updatedAt", "department", "archiveStatus") 
            VALUES {','.join(formatted_rows)}
        '''

    # Send the query using requests
    response = requests.post(
        url=clickhouse_url,
        data=query,
        headers={'Content-Type': 'text/plain'},
        auth=(os.getenv("CLICKHOUSE_USER"), os.getenv("CLICKHOUSE_PASSWORD"))
    )

    if response.status_code != 200:
        raise Exception(f"Failed to load data to ClickHouse: {response.text}")

    return f"Successfully loaded {len(formatted_rows)} rows to ClickHouse"

# Define the DAG
dag = DAG(
    'teachers_to_clickhouse',
    default_args=default_args,
    description='Copy teacher data from Academic Service Postgres to ClickHouse',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['academic', 'teacher']
)

# Define tasks
extract_task = PythonOperator(
    task_id='extract_teachers_from_postgres',
    python_callable=extract_teachers_from_postgres,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_teachers_to_clickhouse',
    python_callable=load_teachers_to_clickhouse,
    provide_context=True,
    dag=dag,
)

# Step 3: Update ETL Timestamp
update_timestamp = PythonOperator(
    task_id='update_etl_timestamp',
    python_callable=update_etl_timestamp
)

# Set task dependencies
extract_task >> load_task >> update_timestamp