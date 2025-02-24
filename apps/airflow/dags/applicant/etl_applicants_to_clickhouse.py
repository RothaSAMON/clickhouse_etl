from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from pymongo import MongoClient
import pandas as pd
import requests
import json
import math
from datetime import datetime

from dotenv import load_dotenv
import os
# Load environment variables from the .env file
load_dotenv()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}
def clean_timestamps(data):
    """Clean timestamp fields to match ClickHouse DateTime format."""
    for row in data:
        for key, value in row.items():
            if isinstance(value, str) and 'T' in value and 'Z' in value:
                try:
                    # Convert ISO 8601 timestamp to ClickHouse-compatible format
                    dt = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
                    row[key] = dt.strftime("%Y-%m-%d %H:%M:%S")  # Remove milliseconds and Z
                except ValueError:
                    try:
                        # Handle case without milliseconds
                        dt = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
                        row[key] = dt.strftime("%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        # Skip if it's not a valid timestamp
                        pass
    return data

def extract_applicants_from_mongodb(**kwargs):
    """Extract applicant's data from MongoDB."""
    # Connect to MongoDB
    client = MongoClient(os.getenv("MONGODB_URL"))
    db = client[f'{os.getenv("DB_APPLICANT")}-{os.getenv("ENVIRONMENT")}']  # Replace with your MongoDB database name
    collection = db['applicants']  # collection name
    
    # Specify fields to select
    cursor = collection.find({}, { 
        "_id": 0, "applicantId": 1, "userKey": 1, "idCard": 1, "enrollToSubject": 1, 
        "enrollToDetail": 1, "lastProfile": 1, "applicantStatus": 1, "source": 1, 
        "admissionFlow": 1, "confirmTarget": 1, "waitApplicantConfirm": 1, "updatedAt":1, 
        "createdAt": 1, "toNotifyApplicant": 1, "schoolId": 1, "userId": 1, "enrollToId": 1 
        })
    data = list(cursor)

    
    # Create a DataFrame for transformation
    df = pd.DataFrame(data)

    # Convert timestamps to string format for JSON serialization
    for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
        df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S')

    client.close()
    
    # Remove MongoDB-specific fields (e.g., _id)
    if '_id' in df.columns:
        df.drop(columns=['_id'], inplace=True)
    
    return df.to_dict('records')

def transform_applicants_data(**kwargs):
    """Transform data."""
    data = kwargs['ti'].xcom_pull(task_ids='extract_applicants_from_mongodb')
    df = pd.DataFrame(data)
    
    # Example transformation: Add a new column with calculated values
    # if 'score' in df.columns:
    #     df['average_score'] = df['score'].mean()
    
    # Convert timestamps to string for JSON serialization
    # for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
    #     df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S')
    
    return df.to_dict('records')

def load_applicants_to_clickhouse(**kwargs):
    """Load data into ClickHouse."""
    data = kwargs['ti'].xcom_pull(task_ids='transform_applicants_data')

    # Clean timestamp fields in the data
    cleaned_data = clean_timestamps(data)

    # Prepare the ClickHouse HTTP endpoint and query
    clickhouse_url = f'{os.getenv("CLICKHOUSE_HOST")}:{os.getenv("CLICKHOUSE_PORT")}'

    formatted_rows = []
    print(f"before formatted_rows")
    for row in cleaned_data:
        formatted_row = {}
        # Convert all fields that are objects (e.g., dict) to JSON strings
        for key, value in row.items():
            if value is None or (isinstance(value, float) and math.isnan(value)):
                formatted_row[key] = 'Null'  # Use 'Null' for SQL NULL in ClickHouse
            elif isinstance(value, str):
                # Wrap strings in single quotes for SQL insertion, ensuring no extra quotes
                formatted_row[key] = f"'{value.replace("'", "''")}'"  # Escape single quotes in strings
            elif isinstance(value, dict):
                # Serialize objects (dicts) as JSON strings and wrap in single quotes
                formatted_row[key] = f"'{json.dumps(value).replace("'", "''")}'"  # Escape single quotes in JSON
            else:
                # Leave other values (like numbers) as is
                formatted_row[key] = value
        formatted_rows.append(formatted_row)

    query = '''
        INSERT INTO clickhouse.applicant ("applicantId", "userKey", "idCard", "enrollToSubject",
        "enrollToDetail", "lastProfile", "applicantStatus", "source", "admissionFlow", "confirmTarget",
        "waitApplicantConfirm", "updatedAt", "createdAt", "toNotifyApplicant", "schoolId", "userId",
        "enrollToId") VALUES 
    '''
    
    rows = ",".join([
        f"""
            ({row['applicantId']}, {row['userKey']}, {row['idCard']}, {row['enrollToSubject']}, {row['enrollToDetail']},
            {row['lastProfile']}, {row['applicantStatus']}, {row['source']}, {row['admissionFlow']}, {row['confirmTarget']},
            {row['waitApplicantConfirm']}, {row['updatedAt']}, {row['createdAt']}, {row['toNotifyApplicant']}, {row['schoolId']},
            {row['userId']}, {row['enrollToId']})
        """ for row in formatted_rows
    ])
    query += rows

    # Log the query before sending it to ClickHouse
    # print(f"Query to ClickHouse: {query}")    
    # Send the query using requests
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
    'applicants_to_clickhouse',
    default_args=default_args,
    description='Extract applicant data from Applicant Service MongoDB, transform it, and load it into ClickHouse',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['applicant']
)

# Define tasks
extract_task = PythonOperator(
    task_id='extract_applicants_from_mongodb',
    python_callable=extract_applicants_from_mongodb,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_applicants_data',
    python_callable=transform_applicants_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_applicants_to_clickhouse',
    python_callable=load_applicants_to_clickhouse,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> load_task
