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
from collections import defaultdict
import uuid
from datetime import datetime


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
def to_float(value):
    if value is None:
        return None  # Handle NoneType explicitly
    try:
        # Convert to float if possible
        return float(value)
    except ValueError:
        # Handle cases where conversion is not possible
        return None
from collections import defaultdict

def format_datetime(dt_str):
    if not dt_str:
        return None

    # Handle the "datetime.date@version=2(1999-09-09)" format.
    if dt_str.startswith("datetime.date@"):
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

def calculate_monthly_subject_scores(evaluations, scores, students, structure_records, subjects):
    """
    Transform raw evaluation, score, student, and structure data into a flattened monthly score record per subject.
    Aggregates custom evaluation scores up to their parent subject evaluations.
    
    Returns a list of dictionaries matching the flattened table:
        student_month_subject_score
    """
    # 1. Build dictionaries for quick lookup
    # Dictionary for evaluations by type
    evaluations_by_id = {eval_rec['evaluationId']: eval_rec for eval_rec in evaluations}
    
    # Create separate dictionaries for each evaluation type
    month_evaluations = {
        eval_id: eval_rec 
        for eval_id, eval_rec in evaluations_by_id.items() 
        if eval_rec.get('type') == 'month'
    }
    
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
    
    logger.info(f'subject_dict: {subject_dict}')
    
    # 2. Create a mapping from custom evaluations to their parent subject evaluations
    custom_to_subject_map = {}
    for custom_id, custom_eval in custom_evaluations.items():
        parent_id = custom_eval.get('parentId')
        if parent_id and parent_id in subject_evaluations:
            custom_to_subject_map[custom_id] = parent_id
    
    # 3. Group scores by (subject_evaluation_id, student_id) and (custom_evaluation_id, student_id)
    subject_grouped_scores = defaultdict(list)
    custom_grouped_scores = defaultdict(list)
    
    for score in scores:
        eval_id = score['evaluationId']
        student_id = score['studentId']
        
        # Process score based on evaluation type
        if eval_id in subject_evaluations:
            # This is a direct subject score
            key = (eval_id, student_id)
            subject_grouped_scores[key].append(score)
        elif eval_id in custom_evaluations:
            # This is a custom evaluation score
            # Store in custom scores
            key = (eval_id, student_id)
            custom_grouped_scores[key].append(score)
            
            # Also map to parent subject for aggregation
            if eval_id in custom_to_subject_map:
                subject_id = custom_to_subject_map[eval_id]
                subject_key = (subject_id, student_id)
                
                # Store a reference to this custom score for aggregation later
                custom_grouped_scores[key][0]['parentSubjectId'] = subject_id
    
    # Process the grouped custom scores to organize by subject
    subject_to_custom_map = defaultdict(list)
    for (custom_id, student_id), score_list in custom_grouped_scores.items():
        if not score_list:
            continue
            
        first_score = score_list[0]
        subject_id = first_score.get('parentSubjectId')
        
        if subject_id:
            # Calculate a single score value for this custom evaluation 
            # (in case there are multiple score entries for same evaluation-student pair)
            score_values = [to_float(s.get('score')) for s in score_list if s.get('score') is not None]
            if not score_values:
                continue
                
            # Calculate average if multiple scores exist
            avg_score = sum(score_values) / len(score_values)
            
            # Get the custom evaluation for reference
            custom_eval = custom_evaluations.get(custom_id, {})
            custom_max_score = to_float(custom_eval.get('maxScore', 100))
            
            # Calculate percentage for this custom evaluation
            percentage = (avg_score / custom_max_score * 100) if custom_max_score > 0 else 0
            
            # Get grade information
            grade, gpa, meaning = get_grade_info(percentage)
            
            # Create record for this custom evaluation score
            custom_record = {
                'evaluationId': custom_id,
                'name': custom_eval.get('name'),
                'score': avg_score,
                'maxScore': custom_max_score,
                'percentage': percentage,
                'grade': grade,
                'gpa': gpa,
                'meaning': meaning,
                'coe': custom_eval.get('coe', 1),
                'description': first_score.get('description')
            }
            
            # Add to the mapping from subject to custom scores
            subject_to_custom_map[(subject_id, student_id)].append(custom_record)
    
    results = []
    # 4. Process each subject-level group to build the flattened records
    processed_subjects = set()
    
    # First process direct subject scores
    for (subject_id, student_id), score_list in subject_grouped_scores.items():
        if not score_list:
            continue
            
        # Calculate final score (average if multiple scores)
        score_values = [to_float(s.get('score')) for s in score_list if s.get('score') is not None]
        if not score_values:
            continue
            
        avg_score = sum(score_values) / len(score_values)
        
        # Get subject evaluation for max score
        subject_eval = subject_evaluations.get(subject_id, {})
        subject_max_score = to_float(subject_eval.get('maxScore', 100))
        
        # Calculate percentage
        percentage = (avg_score / subject_max_score * 100) if subject_max_score > 0 else 0
        
        # Get grade information
        grade, gpa, meaning = get_grade_info(percentage)
        
        # Mark this subject-student pair as processed
        processed_subjects.add((subject_id, student_id))
        
        # Create the record
        record = create_subject_record(
            subject_id, student_id, avg_score, percentage, grade, gpa, meaning, "direct",
            score_list[0], subject_evaluations, month_evaluations, 
            student_dict, structure_dict, subject_dict,
            subject_to_custom_map.get((subject_id, student_id), [])
        )
        
        results.append(record)
    
    # Then process subjects that only have custom scores (no direct scores)
    for (subject_id, student_id), custom_scores in subject_to_custom_map.items():
        # Skip if this subject-student pair was already processed with direct scores
        if (subject_id, student_id) in processed_subjects:
            continue
            
        if not custom_scores:
            continue
        
        # For aggregated scores, we'll keep the original scores but calculate a weighted average
        # based on coefficients and max scores
        
        # Calculate weighted average score based on custom evaluations
        total_weighted_score = 0
        total_weight = 0
        
        for cs in custom_scores:
            # Get percentage
            percentage = cs.get('percentage', 0)
            
            # Get coefficient (weight)
            coef = to_float(cs.get('coe', 1))
            if coef <= 0:
                coef = 1  # Default if missing or invalid
                
            # Add to weighted average calculation
            total_weighted_score += percentage * coef
            total_weight += coef
        
        # Calculate final percentage as weighted average
        final_percentage = total_weighted_score / total_weight if total_weight > 0 else 0
        
        # Get subject evaluation for max score
        subject_eval = subject_evaluations.get(subject_id, {})
        subject_max_score = to_float(subject_eval.get('maxScore', 100))
        
        # Calculate actual score based on percentage and max score
        final_score = (final_percentage * subject_max_score / 100) if subject_max_score > 0 else 0
        
        # Get grade information
        grade, gpa, meaning = get_grade_info(final_percentage)
        
        # Create a placeholder score object for record creation
        placeholder_score = {
            'evaluationId': subject_id,
            'studentId': student_id,
            'structurePath': None,
            'scorerId': custom_scores[0].get('scorerId') if custom_scores else None,
            'markedAt': None,
            'description': None,
            'idCard': None
        }
        
        # Get structure path from a custom score if available
        for (custom_id, stud_id), score_list in custom_grouped_scores.items():
            if stud_id == student_id and score_list and score_list[0].get('parentSubjectId') == subject_id:
                placeholder_score['structurePath'] = score_list[0].get('structurePath')
                placeholder_score['scorerId'] = score_list[0].get('scorerId')
                placeholder_score['markedAt'] = score_list[0].get('markedAt')
                placeholder_score['idCard'] = score_list[0].get('idCard')
                break
        
        # Create the record
        record = create_subject_record(
            subject_id, student_id, final_score, final_percentage, grade, gpa, meaning, "aggregated",
            placeholder_score, subject_evaluations, month_evaluations, 
            student_dict, structure_dict, subject_dict,
            custom_scores
        )
        
        results.append(record)
    
    logger.info(f"Result: {results}")
    return results

def create_subject_record(subject_id, student_id, score_value, percentage, grade, gpa, meaning, score_source, 
                         score_obj, subject_evaluations, month_evaluations, student_dict, 
                         structure_dict, subject_dict, custom_evaluations_list):
    """
    Helper function to create a standardized subject record
    """
    # Get subject evaluation record
    subject_eval = subject_evaluations.get(subject_id, {})
    
    # Get month evaluation record via the parentId of subject evaluation
    month_eval_id = subject_eval.get('parentId')
    month_eval = month_evaluations.get(month_eval_id, {})
    
    # Get student info record
    student_info = student_dict.get(student_id, {})
    
    # Get structure info from the score's structurePath
    structure_record_id = None
    if score_obj.get('structurePath'):
        parts = score_obj['structurePath'].split("#")
        if len(parts) > 1:
            structure_record_id = parts[1]
    
    structure_info = structure_dict.get(structure_record_id, {})
    subject_info = subject_dict.get(structure_record_id, {})
    
    # Convert custom_evaluations_list to JSON string
    custom_evaluations_json = json.dumps(custom_evaluations_list) if custom_evaluations_list else None
    
    # logger.info(f'Date of birth :{type(student_info.get('dob'))}')
    # Build the record
    record = {
        # School & Campus info
        'schoolId': subject_eval.get('schoolId'),
        'campusId': student_info.get('campusId'),
        
        # Structure / Class Info
        'structureRecordId': structure_info.get('structureRecordId'),
        'structureRecordName': structure_info.get('name'),
        'groupStructureId': subject_eval.get('groupStructureId') or student_info.get('groupStructureId') or structure_info.get('groupStructureId'),
        'structurePath': score_obj.get('structurePath'),
        
        # Student Info
        'studentId': student_id,
        'studentFirstName': student_info.get('firstName'),
        'studentLastName': student_info.get('lastName'),
        'studentFirstNameNative': student_info.get('firstNameNative'),
        'studentLastNameNative': student_info.get('lastNameNative'),
        'idCard': student_info.get('idCard'),
        'gender': student_info.get("gender"),
        'dob': student_info.get("dob"),
        
        # Month Evaluation (Parent)
        'monthEvaluationId': month_eval.get('evaluationId'),
        'monthName': month_eval.get('name'),
        'monthStartDate': format_datetime(month_eval.get('attendanceColumn', {}).get('startDate')),
        'monthEndDate': format_datetime(month_eval.get('attendanceColumn', {}).get('endDate')),
        
        # Subject Evaluation (Child)
        'subjectEvaluationId': subject_eval.get('evaluationId'),
        'subjectName': subject_eval.get('name'),
        "subjectNameNative": subject_info.get("nameNative"),
        'subjectMaxScore': to_float(subject_eval.get('maxScore')),
        "credit": subject_info.get("credit"),
        "coe": subject_info.get("coe"),
        "code": subject_info.get("code"),
        
        # Score Info
        'score': score_value,
        'percentage': percentage,
        'grade': grade,
        'gpa': gpa,
        'meaning': meaning,
        'scoreSource': score_source,
        'scorerId': score_obj.get('scorerId'),
        'markedAt': format_datetime(score_obj.get('markedAt')),
        'description': score_obj.get('description'),
        
        # Custom evaluation details (if present)
        'customEvaluationCount': len(custom_evaluations_list),
        'customEvaluations': custom_evaluations_json,
        
        # Timestamp
        'createdAt': format_datetime(subject_eval.get('createdAt'))
    }
    
    return record

# Extracting data
# Extracting from mongo
def extract_data_from_mongodb(**kwargs):
    """Extract evaluations and score data from MongoDB."""
    # Connect to MongoDB
    client = MongoClient(os.getenv("MONGODB_URL"))
    db = client[f'{os.getenv("DB_EVALUATION")}-{os.getenv("ENVIRONMENT")}']  # Replace with your MongoDB database name
    # collection = db['applicants']  # collection name
    
    # Get all evaluations
    evaluations =  list(db['evaluations'].find({}, { 
        "_id": 0, "name": 1, "description": 1, "sort": 1, "maxScore": 1, 
        "coe": 1, "type": 1, "parentId": 1, "schoolId": 1, "campusId": 1,
        "groupStructureId": 1, "structurePath": 1, "evaluationId": 1, "templateId":1, 
        "configGroupId": 1, "referenceId": 1, "createdAt": 1
        }))
    scores =  list(db['scores'].find({}, { 
        "_id": 0, "score": 1, "evaluationId": 1, "studentId": 1, "idCard" : 1,
        "scorerId": 1, "markedAt": 1, "structurePath" : 1
        }))

    # Pass data to the next task
    kwargs['ti'].xcom_push(key='evaluations', value=evaluations)
    kwargs['ti'].xcom_push(key='scores', value=scores)

    client.close()
# Extracting from postgres
def extract_data_from_postgres(**kwargs):
    scores = kwargs['ti'].xcom_pull(key='scores', task_ids='extract_data_from_mongodb')
    logger.info(scores)

    # Extract the IDs from the score records
    structure_ids = set(score['structurePath'].split("#")[1] for score in scores if score.get('structurePath'))
    student_ids = set(score['studentId'] for score in scores if score.get('studentId'))
    logger.info(f"Student IDs: {student_ids}")

    postgres_hook = PostgresHook(postgres_conn_id='academic-staging')
    connection = postgres_hook.get_conn()

    # Use a context manager for the first query (structure data)
    with connection.cursor() as cursor:
        sql_structure = f'''
            SELECT "structureRecordId", "name", "groupStructureId"
            FROM structure_record
            WHERE "structureRecordId" IN ({", ".join("'" + sid + "'" for sid in structure_ids)})
        '''
        cursor.execute(sql_structure)
        structure_data = cursor.fetchall()
        structure_columns = [desc[0] for desc in cursor.description]
        structure_record_records = pd.DataFrame(structure_data, columns=structure_columns).to_dict('records')
        logger.info(f"Structure data: {structure_record_records}")

    # Use a separate cursor for the second query (student data)
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
        logger.info(f"Student data: {student_records}")

    # Use a separate cursor for the second query (subject data)
    with connection.cursor() as cursor:
        sql_subject = f'''
            SELECT "subjectId", "name", "nameNative", "credit", "code", "structureRecordId", "coe"
            FROM subject
            WHERE "structureRecordId" IN ({", ".join("'" + sid + "'" for sid in structure_ids)})
        '''
        cursor.execute(sql_subject)
        subject_data = cursor.fetchall()
        subject_columns = [desc[0] for desc in cursor.description]
        subject_records = pd.DataFrame(subject_data, columns=subject_columns).to_dict('records')
        logger.info(f"Subject data: {subject_records}")
    connection.close()

    # Push the extracted data to XCom
    kwargs['ti'].xcom_push(key='structure_records', value=structure_record_records)
    kwargs['ti'].xcom_push(key='students', value=student_records)
    kwargs['ti'].xcom_push(key='subjects', value=subject_records)



def transform_data(**kwargs):
    """Transform data to calculate average scores"""
    
    # Retrieve data from XCom
    evaluations = kwargs['ti'].xcom_pull(key='evaluations', task_ids='extract_data_from_mongodb')
    scores = kwargs['ti'].xcom_pull(key='scores', task_ids='extract_data_from_mongodb')
    students = kwargs['ti'].xcom_pull(key = "students", task_ids='extract_data_from_postgres')
    structure_records = kwargs['ti'].xcom_pull(key = "structure_records", task_ids='extract_data_from_postgres')
    subjects = kwargs['ti'].xcom_pull(key = "subjects", task_ids='extract_data_from_postgres')


    # print(f"evaluations: { evaluations }")
    # print(f"scores: { scores }")

    # Transform the data
    transformed_data = calculate_monthly_subject_scores(evaluations, scores, students, structure_records, subjects)
    # print(f"transformed data length: { len(transformed_data) }")
    # print(f"transformed data: { transformed_data }")
    # Pass transformed data to the next task
    kwargs['ti'].xcom_push(key='transformed_data', value=transformed_data)

def load_data_to_clickhouse(**kwargs):
    """Load data into ClickHouse."""
    data = kwargs['ti'].xcom_pull(key='transformed_data', task_ids='transform_data')

    # Clean timestamp fields in the data
    # cleaned_data = clean_timestamps(data)
    
    # Prepare the ClickHouse HTTP endpoint and query
    clickhouse_url = f'{os.getenv("CLICKHOUSE_HOST")}:{os.getenv("CLICKHOUSE_PORT")}'

    # def format_value(value):
    #     """Format values with proper type handling."""
    #     if value is None:
    #         return 'NULL'
    #     elif isinstance(value, str):
    #         # Try to determine if the string is a UUID
    #         try:
    #             return f"'{value}'"
    #         except ValueError:
    #             # Escape single quotes for regular strings
    #             escaped_value = value.replace("'", "\\'")
    #             return f"'{escaped_value}'"
    #     else:
    #         return str(value)
    def format_value(value, column_name):
        """Format values with proper type handling for ClickHouse."""
        if column_name == 'dob':
            # Use '0000-00-00 00:00:00' which ClickHouse treats as NULL for DateTime
            return f"'{value}'"
        if value is None:
            return 'NULL'
    
        elif isinstance(value, str):
            # Properly escape single quotes
            escaped_value = value.replace("'", "''")
            return f"'{escaped_value}'"
        else:
            return str(value)

    # Format rows with proper type handling
    formatted_rows = []
    table_keys = [key for key in data[0].keys()]
    for row in data:
        formatted_values = [format_value(row[key], key) for key in row.keys()]
        formatted_rows.append(f"({','.join(formatted_values)})")


    logger.info(data[0:10])
    query = f'INSERT INTO clickhouse.student_month_subject_score_staging ({",".join(table_keys)}) VALUES '
    logger.info(f'Key : {table_keys}')
    logger.info(f'Row: {formatted_rows[6:10]}')
    rows = ",".join(formatted_rows)
    query += rows
    
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
    'scores_by_subject_student_month_to_clickhouse',
    default_args=default_args,
    description='Extract score data from Evaluation Service MongoDB and extract student info from Academic Service Postgres, transform it to score by subject, and load it into ClickHouse',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['evaluation', 'subject', 'score', 'academic']
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
extract_task_mongo >> extract_task_postgres >> transform_task >> load_task;
#
