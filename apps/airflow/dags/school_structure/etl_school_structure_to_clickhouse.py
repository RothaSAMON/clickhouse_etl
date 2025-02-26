from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import datetime
import pandas as pd
import requests
import json

from dotenv import load_dotenv
import os
# Load environment variables from the .env file
load_dotenv()

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

def update_etl_timestamp():
    Variable.set("etl_school_structure_last_run", datetime.now().isoformat())
def format_data(data):
    formatted_rows = []
    for row in data:
        formatted_row = []
        for key, value in row.items():
            if value is None:
                formatted_row.append('NULL')
            elif isinstance(value, str):
                escaped_data = value.replace("'", "\\'")
                formatted_row.append(f"'{escaped_data}'")
            elif isinstance(value, dict):
                json_string = json.dumps(value).replace("'", "\\'")
                formatted_row.append(f"'{json_string}'")
            else:
                formatted_row.append(value)
        formatted_rows.append(f"({','.join(map(str, formatted_row))})")
    return formatted_rows

# Schools callable functions
def extract_schools_from_postgres():
    """Extract data from PostgreSQL."""
    last_run_timestamp = Variable.get("etl_school_structure_last_run", default_var="1970-01-01T00:00:00")

    postgres_hook = PostgresHook(postgres_conn_id='academic-local') #Postgress connection ID that need to be created in Clickhouse
    sql = f'''
        SELECT "schoolId", "name", "nameNative", "code", "url", "email", "phone", "schoolType", "address",
        "status", "province", "country", "minPrice", "maxPrice", "isPublic" 
        FROM school
        WHERE "updatedAt" > '{last_run_timestamp}'
        ORDER BY "updatedAt" DESC;
    '''
    # print(f'sql: {sql}')
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
def load_schools_to_clickhouse(**kwargs):
    """Load data into ClickHouse."""
    data = kwargs['ti'].xcom_pull(task_ids='extract_schools_from_postgres')
    formatted_rows = format_data(data)
    clickhouse_url = f'{os.getenv("CLICKHOUSE_HOST")}:{os.getenv("CLICKHOUSE_PORT")}'
    query = f'''
            INSERT INTO {os.getenv("CLICKHOUSE_DB")}.school 
            ("schoolId", "name", "nameNative", "code", "url", "email", "phone", "schoolType", "address",
            "status", "province", "country", "minPrice", "maxPrice", "isPublic") 
            VALUES {','.join(formatted_rows)}
        '''
    response = requests.post(
        url=clickhouse_url,
        data=query,
        headers={'Content-Type': 'text/plain'},
        auth=(os.getenv("CLICKHOUSE_USER"), os.getenv("CLICKHOUSE_PASSWORD"))
    )
    if response.status_code != 200:
        raise Exception(f"Failed to load data to ClickHouse: {response.text}")

# Campuses callable functions
def extract_campuses_from_postgres():
    """Extract data from PostgreSQL."""
    last_run_timestamp = Variable.get("etl_school_structure_last_run", default_var="1970-01-01T00:00:00")

    postgres_hook = PostgresHook(postgres_conn_id='academic-local') #Postgress connection ID that need to be created in Clickhouse
    sql = f'''
        SELECT "schoolId", "campusId", "name", "nameNative", "code", "phone", "email",
        "address", "isHq", "archiveStatus", "status", "responsibleBy", "structureType"
        FROM campus
        WHERE "updatedAt" > '{last_run_timestamp}'
        ORDER BY "updatedAt" DESC;
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

def load_campuses_to_clickhouse(**kwargs):
    """Load data into ClickHouse."""
    data = kwargs['ti'].xcom_pull(task_ids='extract_campuses_from_postgres')
    formatted_rows = format_data(data)
    clickhouse_url = f'{os.getenv("CLICKHOUSE_HOST")}:{os.getenv("CLICKHOUSE_PORT")}'
    query = f'''
            INSERT INTO {os.getenv("CLICKHOUSE_DB")}.campus 
            ("schoolId", "campusId", "name", "nameNative", "code", "phone", "email",
            "address", "isHq", "archiveStatus", "status", "responsibleBy", "structureType") 
            VALUES {','.join(formatted_rows)}
        '''
    response = requests.post(
        url=clickhouse_url,
        data=query,
        headers={'Content-Type': 'text/plain'},
        auth=(os.getenv("CLICKHOUSE_USER"), os.getenv("CLICKHOUSE_PASSWORD"))
    )
    if response.status_code != 200:
        raise Exception(f"Failed to load data to ClickHouse: {response.text}")

# Group structure callable functions
def extract_group_structures_from_postgres():
    """Extract data from PostgreSQL."""
    last_run_timestamp = Variable.get("etl_school_structure_last_run", default_var="1970-01-01T00:00:00")

    postgres_hook = PostgresHook(postgres_conn_id='academic-local') #Postgress connection ID that need to be created in Clickhouse
    sql = f'''
        SELECT "schoolId", "campusId", "groupStructureId", "name", "nameNative", "code",
        "archiveStatus", "status", "responsibleBy", "structureType" 
        FROM group_structure
        WHERE "updatedAt" > '{last_run_timestamp}'
        ORDER BY "updatedAt" DESC;
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
def load_group_structures_to_clickhouse(**kwargs):
    """Load data into ClickHouse."""
    data = kwargs['ti'].xcom_pull(task_ids='extract_group_structures_from_postgres')
    formatted_rows = format_data(data)
    clickhouse_url = f'{os.getenv("CLICKHOUSE_HOST")}:{os.getenv("CLICKHOUSE_PORT")}'
    query = f'''
            INSERT INTO {os.getenv("CLICKHOUSE_DB")}.group_structure 
            ("schoolId", "campusId", "groupStructureId", "name", "nameNative", "code",
            "archiveStatus", "status", "responsibleBy", "structureType") 
            VALUES {','.join(formatted_rows)}
        '''
    response = requests.post(
        url=clickhouse_url,
        data=query,
        headers={'Content-Type': 'text/plain'},
        auth=(os.getenv("CLICKHOUSE_USER"), os.getenv("CLICKHOUSE_PASSWORD"))
    )
    if response.status_code != 200:
        raise Exception(f"Failed to load data to ClickHouse: {response.text}")

# Structure record callable functions
def extract_structure_records_from_postgres():
    """Extract data from PostgreSQL."""
    last_run_timestamp = Variable.get("etl_school_structure_last_run", default_var="1970-01-01T00:00:00")

    postgres_hook = PostgresHook(postgres_conn_id='academic-local') #Postgress connection ID that need to be created in Clickhouse
    sql = f'''
        SELECT "schoolId", "campusId", "groupStructureId", "structureRecordId", "name", "nameNative", "code",
        "enrollableCategory", "recordType", "tags", "isPromoted", "isFeatured", "isPublic", "isOpen", "startDate", "endDate",
        "archiveStatus", "status", "responsibleBy", "structureType" 
        FROM structure_record
        WHERE "updatedAt" > '{last_run_timestamp}'
        ORDER BY "updatedAt" DESC;
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

def load_structure_records_to_clickhouse(**kwargs):
    """Load data into ClickHouse."""
    data = kwargs['ti'].xcom_pull(task_ids='extract_structure_records_from_postgres')
    formatted_rows = format_data(data)
    clickhouse_url = f'{os.getenv("CLICKHOUSE_HOST")}:{os.getenv("CLICKHOUSE_PORT")}'
    query = f'''
            INSERT INTO {os.getenv("CLICKHOUSE_DB")}.structure_record 
            ("schoolId", "campusId", "groupStructureId", "structureRecordId", "name", "nameNative", "code",
            "enrollableCategory", "recordType", "tags", "isPromoted", "isFeatured", "isPublic", "isOpen", "startDate", "endDate",
            "archiveStatus", "status", "responsibleBy", "structureType" ) 
            VALUES {','.join(formatted_rows)}
        '''
    response = requests.post(
        url=clickhouse_url,
        data=query,
        headers={'Content-Type': 'text/plain'},
        auth=(os.getenv("CLICKHOUSE_USER"), os.getenv("CLICKHOUSE_PASSWORD"))
    )
    if response.status_code != 200:
        raise Exception(f"Failed to load data to ClickHouse: {response.text}")
    
# Define the DAG
dag = DAG(
    'school_structures_to_clickhouse',
    default_args=default_args,
    description='Copy school structures from Academic Service Postgres to ClickHouse',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['academic', 'school', 'campus', 'group_structure', 'structure_record']
)

# Define tasks
extract_schools_task = PythonOperator(
    task_id='extract_schools_from_postgres',
    python_callable=extract_schools_from_postgres,
    dag=dag,
)

extract_campuses_task = PythonOperator(
    task_id='extract_campuses_from_postgres',
    python_callable=extract_campuses_from_postgres,
    dag=dag,
)

extract_group_structures_task = PythonOperator(
    task_id='extract_group_structures_from_postgres',
    python_callable=extract_group_structures_from_postgres,
    dag=dag,
)

extract_structure_records_task = PythonOperator(
    task_id='extract_structure_records_from_postgres',
    python_callable=extract_structure_records_from_postgres,
    dag=dag,
)

load_schools_task = PythonOperator(
    task_id='load_schools_to_clickhouse',
    python_callable=load_schools_to_clickhouse,
    provide_context=True,
    dag=dag,
)
load_campuses_task = PythonOperator(
    task_id='load_campuses_to_clickhouse',
    python_callable=load_campuses_to_clickhouse,
    provide_context=True,
    dag=dag,
)
load_group_structures_task = PythonOperator(
    task_id='load_group_structures_to_clickhouse',
    python_callable=load_group_structures_to_clickhouse,
    provide_context=True,
    dag=dag,
)
load_structure_records_task = PythonOperator(
    task_id='load_structure_records_to_clickhouse',
    python_callable=load_structure_records_to_clickhouse,
    provide_context=True,
    dag=dag,
)
# Step 3: Update ETL Timestamp
update_timestamp = PythonOperator(
    task_id='update_etl_timestamp',
    python_callable=update_etl_timestamp
)

# Set task dependencies
extract_schools_task >> load_schools_task >> extract_campuses_task >> load_campuses_task >> extract_group_structures_task >> load_group_structures_task >> extract_structure_records_task >> load_structure_records_task >> update_timestamp