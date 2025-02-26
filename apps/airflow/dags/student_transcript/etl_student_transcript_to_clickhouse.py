from yaml import load_all
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from pymongo import MongoClient
import pandas as pd
import requests
import json
import logging
from datetime import datetime
import uuid
from collections import defaultdict

from dotenv import load_dotenv
import os
# Load environment variables from the .env file
load_dotenv()
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

def format_datetime(dt_str):
    if not dt_str:
        return None

    # Handle the "datetime.date@version=2(1999-09-09)" format.
    if dt_str and isinstance(dt_str, str) and dt_str.startswith("datetime.date@"):
        # Extract the date part between '(' and ')'
        start = dt_str.find('(')
        end = dt_str.find(')')
        if start != -1 and end != -1:
            date_part = dt_str[start+1:end]
            # Append a default time since ClickHouse DateTime expects a time component.
            return date_part + " 00:00:00"
        else:
            return None

    try:
        # Parse ISO format datetime with milliseconds
        dt = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S.%fZ")
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        try:
            # Try ISO format without milliseconds
            dt = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ")
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None

def to_float(value):
    if value is None:
        return None  # Handle NoneType explicitly
    try:
        # Convert to float if possible
        return float(value)
    except ValueError:
        # Handle cases where conversion is not possible
        return None

def get_grade_info(percentage):
    """
    Determine grade, GPA, and meaning based on percentage score.
    
    Args:
        percentage (float): Score percentage (0-100)
        
    Returns:
        tuple: (grade, gpa, meaning)
    """
    if percentage >= 85:
        return "A", 4.00, "Excellent"
    elif percentage >= 80:
        return "B+", 3.50, "Very Good"
    elif percentage >= 70:
        return "B", 3.00, "Good"
    elif percentage >= 65:
        return "C+", 2.50, "Fairly Good"
    elif percentage >= 50:
        return "C", 2.00, "Fair"
    elif percentage >= 45:
        return "D", 1.50, "Poor"
    elif percentage >= 40:
        return "E", 1.00, "Very Poor"
    else:
        return "F", 0.00, "Failure"

# Extracting data from MongoDB
def extract_data_from_mongodb(**kwargs):
    """Extract evaluations and score data from MongoDB."""
    # Connect to MongoDB
    client = MongoClient(os.getenv("MONGODB_URL"))
    db = client[f'{os.getenv("DB_EVALUATION")}-{os.getenv("ENVIRONMENT")}']
    
    # Get all evaluations
    evaluations = list(db['evaluations'].find({}, {
        "_id": 0, "name": 1, "description": 1, "sort": 1, "maxScore": 1, 
        "coe": 1, "type": 1, "parentId": 1, "schoolId": 1, "campusId": 1,
        "groupStructureId": 1, "structurePath": 1, "evaluationId": 1, "templateId": 1, 
        "configGroupId": 1, "referenceId": 1, "createdAt": 1
    }))
    
    scores = list(db['scores'].find({}, {
        "_id": 0, "score": 1, "evaluationId": 1, "studentId": 1, "idCard": 1,
        "scorerId": 1, "markedAt": 1, "structurePath": 1
    }))

    # Pass data to the next task
    kwargs['ti'].xcom_push(key='evaluations', value=evaluations)
    kwargs['ti'].xcom_push(key='scores', value=scores)

    client.close()

# Extracting data from PostgreSQL
def extract_data_from_postgres(**kwargs):
    scores = kwargs['ti'].xcom_pull(key='scores', task_ids='extract_data_from_mongodb')
    
    # Extract the IDs from the score records
    structure_ids = set(score['structurePath'].split("#")[1] for score in scores if score.get('structurePath'))
    cleaned_structure_ids = {sid for sid in structure_ids if sid != "undefined"}
    student_ids = set(score['studentId'] for score in scores if score.get('studentId'))

    postgres_hook = PostgresHook(postgres_conn_id='academic-staging')
    connection = postgres_hook.get_conn()

    logger.info(f'Structure record ids {structure_ids}')

    # Get structure data
    with connection.cursor() as cursor:
        sql_structure = f'''
            SELECT "structureRecordId", "name", "groupStructureId"
            FROM structure_record
            WHERE "structureRecordId" IN ({", ".join("'" + sid + "'" for sid in cleaned_structure_ids)})
        '''
        logger.info(f"Student sql: {sql_structure}")
        cursor.execute(sql_structure)
        structure_data = cursor.fetchall()
        structure_columns = [desc[0] for desc in cursor.description]
        structure_record_records = pd.DataFrame(structure_data, columns=structure_columns).to_dict('records')

    # Get student data
    with connection.cursor() as cursor:
        sql_student = f'''
            SELECT "studentId", "firstName", "lastName", "firstNameNative", "lastNameNative", "dob", "gender", "campusId", "structureRecordId", "idCard"
            FROM student
            WHERE "studentId" IN ({", ".join("'" + stid + "'" for stid in student_ids)})
        '''
        
        cursor.execute(sql_student)
        student_data = cursor.fetchall()
        student_columns = [desc[0] for desc in cursor.description]
        student_records = pd.DataFrame(student_data, columns=student_columns).to_dict('records')

    # Get subject data
    with connection.cursor() as cursor:
        sql_subject = f'''
            SELECT "subjectId", "name", "nameNative", "credit", "code", "structureRecordId", "coe"
            FROM subject
            WHERE "structureRecordId" IN ({", ".join("'" + sid + "'" for sid in cleaned_structure_ids)})
        '''
        cursor.execute(sql_subject)
        subject_data = cursor.fetchall()
        subject_columns = [desc[0] for desc in cursor.description]
        subject_records = pd.DataFrame(subject_data, columns=subject_columns).to_dict('records')
    
    connection.close()

    # Push the extracted data to XCom
    kwargs['ti'].xcom_push(key='structure_records', value=structure_record_records)
    kwargs['ti'].xcom_push(key='students', value=student_records)
    kwargs['ti'].xcom_push(key='subjects', value=subject_records)

def calculate_subject_scores(evaluations, scores, students, structure_records, subjects):
    """Transform raw data to calculate subject scores and aggregate them by student."""
    
    # 1. Build dictionaries for quick lookup
    evaluations_by_id = {eval_rec['evaluationId']: eval_rec for eval_rec in evaluations}
    
    # Create separate dictionaries for each evaluation type
    subject_evaluations = {
        eval_id: eval_rec 
        for eval_id, eval_rec in evaluations_by_id.items() 
        if eval_rec.get('type') == 'subject'
    }
    
    custom_evaluations = {
        eval_id: eval_rec 
        for eval_id, eval_rec in evaluations_by_id.items() 
        if eval_rec.get('type') == 'custom'
    }
    
    # Dictionary for student information keyed by studentId
    student_dict = {stu['studentId']: stu for stu in students}
    
    # Dictionary for structure records keyed by structureRecordId
    structure_dict = {s['structureRecordId']: s for s in structure_records}
    
    # Dictionary for subjects keyed by structureRecordId
    subject_dict = {s['structureRecordId']: s for s in subjects}
    
    # Map custom evaluations to their parent subjects
    custom_by_parent = defaultdict(list)
    for eval_id, eval_rec in custom_evaluations.items():
        parent_id = eval_rec.get('parentId')
        if parent_id and parent_id in subject_evaluations:
            custom_by_parent[parent_id].append(eval_id)
    
    # 2. Group scores by (evaluation_id, student_id)
    grouped_scores = defaultdict(list)
    
    for score in scores:
        eval_id = score['evaluationId']
        student_id = score['studentId']
        
        # Process all evaluation types
        key = (eval_id, student_id)
        grouped_scores[key].append(score)
    
    # 3. Process custom scores first to calculate subject scores
    subject_custom_scores = {}
    
    for subject_id, custom_eval_ids in custom_by_parent.items():
        for student_id in {score['studentId'] for score in scores}:
            custom_percentages = []
            
            for custom_id in custom_eval_ids:
                key = (custom_id, student_id)
                if key not in grouped_scores:
                    continue
                
                custom_score_list = grouped_scores[key]
                if not custom_score_list:
                    continue
                
                # Calculate average for this custom evaluation
                score_values = [to_float(s.get('score')) for s in custom_score_list if s.get('score') is not None]
                clean_score_values = [0 if score is None else score for score in score_values]
                if not clean_score_values:
                    continue
                
                custom_avg = sum(clean_score_values) / len(clean_score_values)
                
                # Get custom evaluation for max score
                custom_eval = custom_evaluations.get(custom_id, {})
                custom_max = to_float(custom_eval.get('maxScore', 100))
                
                # Calculate percentage
                custom_percentage = (custom_avg / custom_max * 100) if custom_max > 0 else 0
                custom_percentages.append(custom_percentage)
            
            # Only calculate if we have custom scores
            if custom_percentages:
                # Average of all custom percentages
                avg_custom_percentage = sum(custom_percentages) / len(custom_percentages)
                
                # Get subject max score to convert percentage back to absolute score
                subject_eval = subject_evaluations.get(subject_id, {})
                subject_max = to_float(subject_eval.get('maxScore', 100))
                
                # Calculate final subject score based on custom percentages
                final_subject_score = (avg_custom_percentage * subject_max) / 100
                
                # Store for later use
                subject_custom_scores[(subject_id, student_id)] = final_subject_score
    
    # 4. Process scores to create detailed subject records
    subject_details_by_student = defaultdict(list)
    scorers_by_student = {}
    marked_at_by_student = {}
    
    # Process each subject level group
    for subject_id, subject_eval in subject_evaluations.items():
        for student_id in {score['studentId'] for score in scores}:
            key = (subject_id, student_id)
            
            # Determine final score prioritizing direct scores over custom-derived scores
            final_score = None
            score_list = grouped_scores.get(key, [])
            latest_score = None
            
            if score_list:
                # Use direct subject scores if available
                score_values = [to_float(s.get('score')) for s in score_list if s.get('score') is not None]
                clean_score_values = [0 if score is None else score for score in score_values]
                if clean_score_values:
                    final_score = sum(clean_score_values) / len(clean_score_values)
                    latest_score = score_list[0]  # Assuming scores are in chronological order
            
            # If no direct scores, use aggregate of custom scores if available
            if final_score is None and key in subject_custom_scores:
                final_score = subject_custom_scores[key]
                # Need to find a latest score for marker info
                for custom_id in custom_by_parent.get(subject_id, []):
                    custom_scores = grouped_scores.get((custom_id, student_id), [])
                    if custom_scores and (latest_score is None or 
                            custom_scores[0].get('markedAt', '') > latest_score.get('markedAt', '')):
                        latest_score = custom_scores[0]
            
            # Skip if no score available
            if final_score is None or latest_score is None:
                continue
            
            # Get subject evaluation for max score
            subject_max_score = to_float(subject_eval.get('maxScore', 100))
            
            # Calculate percentage
            percentage = (final_score / subject_max_score * 100) if subject_max_score > 0 else 0
            
            # Get grade information
            grade, gpa, meaning = get_grade_info(percentage)
            
            # Get structure path from score if available
            structure_record_id = None
            if latest_score.get('structurePath'):
                parts = latest_score['structurePath'].split("#")
                if len(parts) > 1:
                    structure_record_id = parts[1]
            
            # Get subject info from subjects dictionary
            subject_info = None
            for subj in subjects:
                if subj.get('structureRecordId') == structure_record_id:
                    subject_info = subj
                    break
            
            # Store scorer and marked_at for latest update
            scorers_by_student[student_id] = latest_score.get('scorerId')
            marked_at_by_student[student_id] = format_datetime(latest_score.get('markedAt'))
            
            # --- Determine parent evaluation info (month or semester) ---
            subject_parent_name = ""
            subject_parent_evaluation_id = None
            subject_parent_type = ""
            month_name = ""
            month_evaluation_id = None
            semester_name = ""
            semester_evaluation_id = None
            
            # First check direct parent
            parent_id = subject_eval.get('parentId')
            if parent_id and parent_id != "na":
                parent_eval = evaluations_by_id.get(parent_id, {})
                subject_parent_name = parent_eval.get('name', "")
                subject_parent_evaluation_id = parent_eval.get('evaluationId')
                subject_parent_type = parent_eval.get('type', '')
                
                # Check if parent is month or semester and set accordingly
                if subject_parent_type == 'month':
                    month_name = subject_parent_name
                    month_evaluation_id = subject_parent_evaluation_id
                    
                    # Check if month has a semester parent
                    month_parent_id = parent_eval.get('parentId')
                    if month_parent_id and month_parent_id != "na":
                        semester_eval = evaluations_by_id.get(month_parent_id, {})
                        if semester_eval.get('type') == 'semester':
                            semester_name = semester_eval.get('name', "")
                            semester_evaluation_id = semester_eval.get('evaluationId')
                
                elif subject_parent_type == 'semester':
                    semester_name = subject_parent_name
                    semester_evaluation_id = subject_parent_evaluation_id
            
            # Create a subject detail tuple with parent info included
            subject_detail = (
                subject_id,                                          # subjectEvaluationId
                subject_eval.get('name', ''),                        # subjectName
                subject_info.get('nameNative', '') if subject_info else '',  # subjectNameNative
                subject_info.get('code', '') if subject_info else '',         # code
                float(subject_info.get('credit', 0)) if subject_info else 0,    # credit
                final_score,                                         # score
                subject_eval.get('maxScore', ''),
                percentage,                                          # percentage
                grade,                                               # grade
                meaning,                                             # meaning
                gpa,                                                 # gpa
                subject_parent_name,                                 # subjectParentName (from direct parent)
                subject_parent_evaluation_id,                        # subjectParentEvaluationId
                subject_parent_type,                                 # subjectParentType
                month_name,                                          # monthName
                month_evaluation_id,                                 # monthEvaluationId

                semester_name,                                       # semesterName
                semester_evaluation_id,                               # semesterEvaluationId

            )
            
            # Add to student's subject details, keyed by (student_id, structure_record_id)
            key = (student_id, structure_record_id)
            subject_details_by_student[key].append(subject_detail)
    
    # 5. Aggregate into student transcript records
    transcript_records = []
    
    for (student_id, structure_record_id), subject_details in subject_details_by_student.items():
        if not subject_details:
            continue
        
        # Get student info
        student_info = student_dict.get(student_id, {})
        
        # Get structure info
        structure_info = structure_dict.get(structure_record_id, {})
        
        # Calculate totals
        total_credits = sum(detail[4] for detail in subject_details)
        weighted_gpa_sum = sum(detail[4] * detail[10] for detail in subject_details)
        total_gpa = weighted_gpa_sum / total_credits if total_credits > 0 else 0
        
        # Create the transcript record
        record = {
            # School & Campus info
            'schoolId': subject_evaluations.get(subject_details[0][0], {}).get('schoolId'),
            'campusId': student_info.get('campusId'),
            
            # Structure / Class Info
            'structureRecordId': structure_record_id,
            'structureRecordName': structure_info.get('name'),
            'groupStructureId': structure_info.get('groupStructureId'),
            'structurePath': f"#{structure_record_id}",
            
            # Student Info
            'studentId': student_id,
            'studentFirstName': student_info.get('firstName'),
            'studentLastName': student_info.get('lastName'),
            'studentFirstNameNative': student_info.get('firstNameNative'),
            'studentLastNameNative': student_info.get('lastNameNative'),
            'idCard': student_info.get('idCard'),
            'gender': student_info.get("gender"),
            'dob': student_info.get("dob"),
            
            # Subject Details (array of tuples with parent info)
            'subjectDetails': subject_details,
            
            # Totals
            'totalCredits': total_credits,
            'totalGPA': total_gpa,
            'subjectCount': len(subject_details),
            
            # Additional Info
            'scorerId': scorers_by_student.get(student_id),
            'markedAt': marked_at_by_student.get(student_id),
            
            # Timestamp
            'createdAt': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        transcript_records.append(record)
    
    logger.info(f'transcript_record {transcript_records[20:30]}')
    logger.info(f'transcript_record {len(transcript_records)}')

    return transcript_records

def transform_data(**kwargs):
    """Transform data to calculate student transcripts"""
    
    # Retrieve data from XCom
    evaluations = kwargs['ti'].xcom_pull(key='evaluations', task_ids='extract_data_from_mongodb')
    scores = kwargs['ti'].xcom_pull(key='scores', task_ids='extract_data_from_mongodb')
    students = kwargs['ti'].xcom_pull(key='students', task_ids='extract_data_from_postgres')
    structure_records = kwargs['ti'].xcom_pull(key='structure_records', task_ids='extract_data_from_postgres')
    subjects = kwargs['ti'].xcom_pull(key='subjects', task_ids='extract_data_from_postgres')

    # Transform the data
    transformed_data = calculate_subject_scores(evaluations, scores, students, structure_records, subjects)
    logger.info(f"Generated {len(transformed_data)} transcript records")
    
    # Pass transformed data to the next task
    kwargs['ti'].xcom_push(key='transformed_data', value=transformed_data)

def load_data_to_clickhouse(**kwargs):
    """Load data into ClickHouse."""
    data = kwargs['ti'].xcom_pull(key='transformed_data', task_ids='transform_data')
    
    if not data:
        logger.warning("No data to load into ClickHouse")
        return
    
    # Prepare the ClickHouse HTTP endpoint and query
    clickhouse_url = f'{os.getenv("CLICKHOUSE_HOST")}:{os.getenv("CLICKHOUSE_PORT")}'
    
    def format_value(value, key):
        """Format values for ClickHouse with proper type handling."""
        if value is None:
            return "NULL"  # Ensure None is converted to NULL

        if key == 'dob':
            # Use '0000-00-00 00:00:00' for missing DateTime values
            return f"'{value}'"

        if key == 'subjectDetails':
            formatted_tuples = []
            for tup in value:
                elements = []
                for i, elem in enumerate(tup):
                    if elem is None:  # Convert None inside tuples as well
                        elements.append("NULL")
                    elif i == 0:  # UUID
                        elements.append(f"'{elem}'")
                    elif isinstance(elem, str):
                        escaped = elem.replace("'", "''")
                        elements.append(f"'{escaped}'")
                    else:
                        elements.append(str(elem))
                formatted_tuples.append(f"({','.join(elements)})")
            
            return f"[{','.join(formatted_tuples)}]"

        elif isinstance(value, str):
            escaped_value = value.replace("'", "''")
            return f"'{escaped_value}'"
        
        return str(value)

    # Build the formatted row values
    formatted_rows = []
    table_keys = list(data[0].keys())

    for row in data:
        formatted_values = [format_value(row[key], key) for key in table_keys]
        formatted_rows.append(f"({','.join(formatted_values)})")

    # Construct the query
    query = f'INSERT INTO clickhouse.student_transcript_staging ({",".join(table_keys)}) VALUES '
    query += ",".join(formatted_rows)
    
    # Send the query using requests
    try:
        response = requests.post(
            url=clickhouse_url,
            data=query,
            headers={'Content-Type': 'text/plain'},
            auth=(os.getenv("CLICKHOUSE_USER"), os.getenv("CLICKHOUSE_PASSWORD"))
        )
        
        if response.status_code != 200:
            error_msg = f"Failed to load data to ClickHouse: {response.text}"
            logger.error(error_msg)
            raise Exception(error_msg)
        
        logger.info(f"Successfully loaded {len(data)} records into student_transcript table")
    except Exception as e:
        logger.error(f"Error loading data to ClickHouse: {str(e)}")
        raise

# Define the DAG
dag = DAG(
    'student_transcript_etl',
    default_args=default_args,
    description='Extract score data, transform it into student transcripts, and load into ClickHouse',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['evaluation', 'transcript', 'academic']
)

# Define tasks
extract_task_mongo = PythonOperator(
    task_id='extract_data_from_mongodb',
    python_callable=extract_data_from_mongodb,
    provide_context=True,
    dag=dag,
)

extract_task_postgres = PythonOperator(
    task_id='extract_data_from_postgres',
    python_callable=extract_data_from_postgres,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data_to_clickhouse',
    python_callable=load_data_to_clickhouse,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
extract_task_mongo >> extract_task_postgres >> transform_task  >> load_task
# 