# from airflow import DAG
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.providers.http.hooks.http import HttpHook
# from airflow.operators.python import PythonOperator
# from airflow.utils.dates import days_ago
# from airflow.models import Variable
# from datetime import datetime
# import pandas as pd
# import requests
# import os
# import json

# from dotenv import load_dotenv
# # Load environment variables from the .env file
# load_dotenv()

# # Define default arguments for the DAG
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'retries': 1,
# }

# def update_etl_timestamp():
#     Variable.set("etl_subjects_last_run", datetime.now().isoformat())
# def format_data(data):
#     formatted_rows = []
#     for row in data:
#         formatted_row = []
#         for key, value in row.items():
#             if value is None:
#                 formatted_row.append('NULL')
#             elif isinstance(value, str):
#                 escaped_data = value.replace("'", "\\'")
#                 formatted_row.append(f"'{escaped_data}'")
#             elif isinstance(value, dict):
#                 json_string = json.dumps(value).replace("'", "\\'")
#                 formatted_row.append(f"'{json_string}'")
#             else:
#                 formatted_row.append(value)
#         formatted_rows.append(f"({','.join(map(str, formatted_row))})")
#     return formatted_rows

# def extract_subjects_from_postgres():
#     """Extract data from PostgreSQL."""
#     last_run_timestamp = Variable.get("etl_subjects_last_run", default_var="1970-01-01T00:00:00")

#     postgres_hook = PostgresHook(postgres_conn_id='academic-local') #Postgress connection ID that need to be created in Clickhouse
#     sql = f'''
#         SELECT "schoolId", "campusId", "groupStructureId", "structureRecordId",
#         "subjectId", "curriculumId", "name", "nameNative", "description", "credit",
#         "code", "practiceHour", "theoryHour", "fieldHour", "totalHour",
#         "archiveStatus", "lmsCourseId"
#         FROM subject
#         WHERE "updatedAt" > '{last_run_timestamp}'
#         ORDER BY "updatedAt" DESC;
#     '''
#     connection = postgres_hook.get_conn()
#     cursor = connection.cursor()
#     cursor.execute(sql)
#     data = cursor.fetchall()
#     columns = [desc[0] for desc in cursor.description]
#     df = pd.DataFrame(data, columns=columns)

#     # Convert timestamps to string format for JSON serialization
#     for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
#         df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S')

#     cursor.close()
#     connection.close()
#     return df.to_dict('records')

# def load_subjects_to_clickhouse(**kwargs):
#     """Load data into ClickHouse."""
#     data = kwargs['ti'].xcom_pull(task_ids='extract_subjects_from_postgres')

#     # Replace Python None with SQL NULL
#     formatted_rows = format_data(data)

#     # print(f"formatted_rows: {formatted_rows[0]}")
#     # Prepare the ClickHouse HTTP endpoint and query
#     clickhouse_url = f'{os.getenv("CLICKHOUSE_HOST")}:{os.getenv("CLICKHOUSE_PORT")}'
#     query = f'''
#             INSERT INTO {os.getenv("CLICKHOUSE_DB")}.subject 
#             ("schoolId", "campusId", "groupStructureId", "structureRecordId",
#             "subjectId", "curriculumId", "name", "nameNative", "description", "credit",
#             "code", "practiceHour", "theoryHour", "fieldHour", "totalHour",
#             "archiveStatus", "lmsCourseId") 
#             VALUES {','.join(formatted_rows)}
#         '''
#     response = requests.post(
#         url=clickhouse_url,
#         data=query,
#         headers={'Content-Type': 'text/plain'},
#         auth=(os.getenv("CLICKHOUSE_USER"), os.getenv("CLICKHOUSE_PASSWORD"))
#     )

#     if response.status_code != 200:
#         raise Exception(f"Failed to load data to ClickHouse: {response.text}")

# # Define the DAG
# dag = DAG(
#     'subjects_to_clickhouse',
#     default_args=default_args,
#     description='Copy subject data from Academic Service Postgres to ClickHouse',
#     schedule_interval='@daily',
#     start_date=days_ago(1),
#     catchup=False,
#     tags=['academic', 'subject']
# )

# # Define tasks
# extract_task = PythonOperator(
#     task_id='extract_subjects_from_postgres',
#     python_callable=extract_subjects_from_postgres,
#     dag=dag,
# )

# load_task = PythonOperator(
#     task_id='load_subjects_to_clickhouse',
#     python_callable=load_subjects_to_clickhouse,
#     provide_context=True,
#     dag=dag,
# )
# # Step 3: Update ETL Timestamp
# update_timestamp = PythonOperator(
#     task_id='update_etl_timestamp',
#     python_callable=update_etl_timestamp
# )

# # Set task dependencies
# extract_task >> load_task >> update_timestamp

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import datetime
import pandas as pd
import requests
import os
import json

from dotenv import load_dotenv
# Load environment variables from the .env file
load_dotenv()

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

def update_etl_timestamp():
    Variable.set("etl_subjects_last_run", datetime.now().isoformat())

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

def extract_subjects_from_postgres():
    """Extract data from PostgreSQL."""
    # Removing the timestamp condition
    postgres_hook = PostgresHook(postgres_conn_id='academic-local') #Postgress connection ID
    sql = '''
        SELECT "schoolId", "campusId", "groupStructureId", "structureRecordId",
        "subjectId", "curriculumId", "name", "nameNative", "description", "credit",
        "code", "practiceHour", "theoryHour", "fieldHour", "totalHour",
        "archiveStatus", "lmsCourseId"
        FROM subject
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

def load_subjects_to_clickhouse(**kwargs):
    """Load data into ClickHouse."""
    data = kwargs['ti'].xcom_pull(task_ids='extract_subjects_from_postgres')

    # Replace Python None with SQL NULL
    formatted_rows = format_data(data)

    # Prepare the ClickHouse HTTP endpoint and query
    clickhouse_url = f'{os.getenv("CLICKHOUSE_HOST")}:{os.getenv("CLICKHOUSE_PORT")}'
    query = f'''
            INSERT INTO {os.getenv("CLICKHOUSE_DB")}.subject 
            ("schoolId", "campusId", "groupStructureId", "structureRecordId",
            "subjectId", "curriculumId", "name", "nameNative", "description", "credit",
            "code", "practiceHour", "theoryHour", "fieldHour", "totalHour",
            "archiveStatus", "lmsCourseId") 
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
    'subjects_to_clickhouse',
    default_args=default_args,
    description='Copy subject data from Academic Service Postgres to ClickHouse',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['academic', 'subject']
)

# Define tasks
extract_task = PythonOperator(
    task_id='extract_subjects_from_postgres',
    python_callable=extract_subjects_from_postgres,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_subjects_to_clickhouse',
    python_callable=load_subjects_to_clickhouse,
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
