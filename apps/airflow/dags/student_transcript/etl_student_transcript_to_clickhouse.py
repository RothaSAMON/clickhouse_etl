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
        start = dt_str.find('(')
        end = dt_str.find(')')
        if start != -1 and end != -1:
            date_part = dt_str[start+1:end]
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
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
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

# Extracting data from MongoDB with improved query filtering
def extract_data_from_mongodb(**kwargs):
    """Extract evaluations and score data from MongoDB with hierarchy intact."""
    # Connect to MongoDB
    client = MongoClient(os.getenv("MONGODB_URL"))
    db = client[f'{os.getenv("DB_EVALUATION")}-{os.getenv("ENVIRONMENT")}']
    
    # Get all evaluations - now including parentId for establishing hierarchy
    evaluations = list(db['evaluations'].find({}, {
        "_id": 0, "name": 1, "description": 1, "sort": 1, "maxScore": 1, 
        "coe": 1, "type": 1, "parentId": 1, "schoolId": 1, "campusId": 1,
        "groupStructureId": 1, "structurePath": 1, "evaluationId": 1, "templateId": 1, 
        "configGroupId": 1, "referenceId": 1, "createdAt": 1, "subjectId": 1, "credit": 1
    }))
    
    # Get scores with expanded fields
    scores = list(db['scores'].find({}, {
        "_id": 0, "score": 1, "evaluationId": 1, "studentId": 1, "idCard": 1,
        "scorerId": 1, "markedAt": 1, "structurePath": 1, "semesterId": 1
    }))

    # Pass data to the next task
    kwargs['ti'].xcom_push(key='evaluations', value=evaluations)
    kwargs['ti'].xcom_push(key='scores', value=scores)

    # Log summary statistics
    logger.info(f"Extracted {len(evaluations)} evaluations and {len(scores)} scores from MongoDB")
    logger.info(f"Evaluation types: {set(e.get('type') for e in evaluations if 'type' in e)}")
    
    client.close()

def is_valid_uuid(val):
    try:
        return str(uuid.UUID(val)) == val
    except (ValueError, TypeError, AttributeError):
        return False

def extract_data_from_postgres(**kwargs):
    """Extract structure, student and subject data from Postgres."""
    evaluations = kwargs['ti'].xcom_pull(key='evaluations', task_ids='extract_data_from_mongodb')
    scores = kwargs['ti'].xcom_pull(key='scores', task_ids='extract_data_from_mongodb')
    
    # Extract unique structure IDs from evaluations and scores
    structure_ids = set()
    for eval_item in evaluations:
        if eval_item.get('structurePath'):
            parts = eval_item['structurePath'].split("#")
            if len(parts) > 1 and is_valid_uuid(parts[1]):
                structure_ids.add(parts[1])
    
    for score in scores:
        if score.get('structurePath'):
            parts = score['structurePath'].split("#")
            if len(parts) > 1 and is_valid_uuid(parts[1]):
                structure_ids.add(parts[1])
    
    structure_ids = list(structure_ids)
    
    # Extract unique student IDs from scores
    student_ids = {score['studentId'] for score in scores if score.get('studentId') and is_valid_uuid(score['studentId'])}
    student_ids = list(student_ids)
    
    if not structure_ids or not student_ids:
        logger.warning("No valid UUIDs found for structure_ids or student_ids")
        if not structure_ids:
            logger.warning("No valid structure IDs found")
        if not student_ids:
            logger.warning("No valid student IDs found")
        return
    
    # Create database connection
    postgres_hook = PostgresHook(postgres_conn_id='academic-staging')
    connection = postgres_hook.get_conn()

    # Query structure records
    with connection.cursor() as cursor:
        sql_structure = '''
            SELECT "structureRecordId", "name", "groupStructureId"
            FROM structure_record
            WHERE "structureRecordId" = ANY(%s::uuid[])
        '''
        cursor.execute(sql_structure, (structure_ids,))
        structure_data = cursor.fetchall()
        structure_columns = [desc[0] for desc in cursor.description]
        structure_record_records = pd.DataFrame(structure_data, columns=structure_columns).to_dict('records')

    # Query student information
    with connection.cursor() as cursor:
        sql_student = '''
            SELECT "studentId", "firstName", "lastName", "firstNameNative", "lastNameNative", 
                   "dob", "gender", "campusId", "structureRecordId", "idCard"
            FROM student
            WHERE "studentId" = ANY(%s::uuid[])
        '''
        cursor.execute(sql_student, (student_ids,))
        student_data = cursor.fetchall()
        student_columns = [desc[0] for desc in cursor.description]
        student_records = pd.DataFrame(student_data, columns=student_columns).to_dict('records')

    # Query subject information with more details
    with connection.cursor() as cursor:
        sql_subjects = '''
            SELECT "subjectId", "name", "nameNative", "code", "credit", "structureRecordId", "schoolId"
            FROM subject
            WHERE "structureRecordId" = ANY(%s::uuid[])
        '''
        cursor.execute(sql_subjects, (structure_ids,))
        subject_data = cursor.fetchall()
        subject_columns = [desc[0] for desc in cursor.description]
        subjects = pd.DataFrame(subject_data, columns=subject_columns).to_dict('records')

    connection.close()

    # Push data to XCom
    kwargs['ti'].xcom_push(key='structure_records', value=structure_record_records)
    kwargs['ti'].xcom_push(key='students', value=student_records)
    kwargs['ti'].xcom_push(key='subjects', value=subjects)
    
    # Log summary statistics
    logger.info(f"Extracted {len(structure_record_records)} structure records from Postgres")
    logger.info(f"Extracted {len(student_records)} students from Postgres")
    logger.info(f"Extracted {len(subjects)} subjects from Postgres")

def transform_data(**kwargs):
    """
    Transform data to calculate student transcripts
    with improved hierarchy handling and GPA calculation
    """
    # Retrieve data from XCom
    evaluations = kwargs['ti'].xcom_pull(key='evaluations', task_ids='extract_data_from_mongodb')
    scores = kwargs['ti'].xcom_pull(key='scores', task_ids='extract_data_from_mongodb')
    students = kwargs['ti'].xcom_pull(key='students', task_ids='extract_data_from_postgres')
    structure_records = kwargs['ti'].xcom_pull(key='structure_records', task_ids='extract_data_from_postgres')
    subjects = kwargs['ti'].xcom_pull(key='subjects', task_ids='extract_data_from_postgres')
    
    if not evaluations or not scores or not students or not structure_records or not subjects:
        logger.error("Missing required data for transformation")
        return
    
    # 1. Build hierarchical relationships between evaluations
    # Create dictionaries for quick lookup
    evaluations_by_id = {eval_rec['evaluationId']: eval_rec for eval_rec in evaluations if 'evaluationId' in eval_rec}
    
    # Create mappings for each evaluation type
    semester_evaluations = {ev_id: ev for ev_id, ev in evaluations_by_id.items() 
                           if ev.get('type') == 'semester'}
    subject_evaluations = {ev_id: ev for ev_id, ev in evaluations_by_id.items() 
                           if ev.get('type') == 'subject'}
    custom_evaluations = {ev_id: ev for ev_id, ev in evaluations_by_id.items() 
                          if ev.get('type') == 'custom'}
    
    # Map custom evaluations to their parent subject
    custom_to_subject = {}
    for custom_id, custom_eval in custom_evaluations.items():
        parent_id = custom_eval.get('parentId')
        if parent_id and parent_id in subject_evaluations:
            custom_to_subject[custom_id] = parent_id
    
    # Map subjects to their parent semester
    subject_to_semester = {}
    for subject_id, subject_eval in subject_evaluations.items():
        parent_id = subject_eval.get('parentId')
        if parent_id and parent_id in semester_evaluations:
            subject_to_semester[subject_id] = parent_id
    
    # 2. Create dictionaries for student, structure, and subject information
    student_dict = {stu['studentId']: stu for stu in students if 'studentId' in stu}
    structure_dict = {s['structureRecordId']: s for s in structure_records if 'structureRecordId' in s}
    subject_dict = {}
    for subj in subjects:
        subj_id = subj.get('subjectId')
        if subj_id:
            subject_dict[subj_id] = subj
    
    # 3. Group scores by student and link to subjects
    # First, map custom evaluation scores to their subject parent
    subject_scores_by_student = defaultdict(list)
    
    for score in scores:
        eval_id = score.get('evaluationId')
        student_id = score.get('studentId')
        
        if not eval_id or not student_id:
            continue
            
        # Get the structure path from the score
        structure_record_id = None
        if score.get('structurePath'):
            parts = score['structurePath'].split("#")
            if len(parts) > 1 and is_valid_uuid(parts[1]):
                structure_record_id = parts[1]
        
        # For custom evaluations, map them to subject evaluations
        if eval_id in custom_evaluations:
            subject_id = custom_to_subject.get(eval_id)
            if subject_id:
                # Store the score with its parent subject ID
                key = (student_id, structure_record_id, subject_id)
                subject_scores_by_student[key].append({
                    'score': score.get('score'),
                    'evaluationId': eval_id,
                    'scorerId': score.get('scorerId'),
                    'markedAt': score.get('markedAt'),
                    'maxScore': custom_evaluations[eval_id].get('maxScore')
                })
        
        # Direct subject evaluation scores
        elif eval_id in subject_evaluations:
            key = (student_id, structure_record_id, eval_id)
            subject_scores_by_student[key].append({
                'score': score.get('score'),
                'evaluationId': eval_id,
                'scorerId': score.get('scorerId'),
                'markedAt': score.get('markedAt'),
                'maxScore': subject_evaluations[eval_id].get('maxScore')
            })
    
    # 4. Calculate subject-level scores and GPA for each student
    transcript_by_student_structure = defaultdict(lambda: {
        'studentInfo': {},
        'structureInfo': {},
        'subjectDetails': [],
        'totalCredits': 0.0,
        'weightedGpaSum': 0.0,
        'scorerId': None,
        'markedAt': None
    })
    
    for (student_id, structure_record_id, subject_id), scores_list in subject_scores_by_student.items():
        if not student_id or not structure_record_id or not subject_id:
            continue
            
        # Get student info
        student_info = student_dict.get(student_id, {})
        if not student_info:
            continue
            
        # Get structure info
        structure_info = structure_dict.get(structure_record_id, {})
        if not structure_info:
            continue
            
        # Get subject evaluation
        subject_eval = subject_evaluations.get(subject_id, {})
        if not subject_eval:
            continue
        
        # Find the matching subject from subject_dict
        matching_subject = None
        for subj in subjects:
            # Match by reference from evaluation or by name
            if (subj.get('subjectId') == subject_eval.get('subjectId') or 
                subj.get('name') == subject_eval.get('name')):
                if subj.get('structureRecordId') == structure_record_id:
                    matching_subject = subj
                    break
        
        # Calculate the average score for this subject
        valid_scores = []
        for score_data in scores_list:
            score_value = score_data.get('score')
            # Handle different score formats
            try:
                if isinstance(score_value, (int, float)):
                    valid_scores.append(float(score_value))
                elif isinstance(score_value, str):
                    if score_value.replace('.', '', 1).isdigit():
                        valid_scores.append(float(score_value))
            except (ValueError, TypeError):
                continue
        
        if not valid_scores:
            continue
            
        avg_score = sum(valid_scores) / len(valid_scores)
        
        # Get subject max score and credit
        subject_max_score = to_float(subject_eval.get('maxScore', 100))
        subject_credit = 0.0
        
        # Try to get credit from subject record first, then from the evaluation
        if matching_subject and matching_subject.get('credit') is not None:
            try:
                subject_credit = float(matching_subject['credit'])
            except (ValueError, TypeError):
                pass
        
        if subject_credit == 0 and subject_eval.get('credit') is not None:
            try:
                subject_credit = float(subject_eval['credit'])
            except (ValueError, TypeError):
                pass
        
        # Calculate percentage and grade
        percentage = (avg_score / subject_max_score * 100) if subject_max_score else 0
        grade, gpa, meaning = get_grade_info(percentage)
        
        # Get subject name information
        subject_name = subject_eval.get('name', '')
        subject_name_native = ''
        subject_code = ''
        
        if matching_subject:
            subject_name = matching_subject.get('name', subject_name)
            subject_name_native = matching_subject.get('nameNative', '')
            subject_code = matching_subject.get('code', '')
        
        # Create subject detail tuple
        subject_detail = (
            subject_id,  # subjectEvaluationId
            subject_name,  # subjectName
            subject_name_native,  # subjectNameNative
            subject_code,  # code
            subject_credit,  # credit
            avg_score,  # score
            percentage,  # percentage
            grade,  # grade
            meaning,  # meaning
            gpa  # gpa
        )
        
        # Update the transcript record for this student and structure
        key = (student_id, structure_record_id)
        
        if not transcript_by_student_structure[key]['studentInfo']:
            transcript_by_student_structure[key]['studentInfo'] = {
                'studentId': student_id,
                'firstName': student_info.get('firstName', ''),
                'lastName': student_info.get('lastName', ''),
                'firstNameNative': student_info.get('firstNameNative', ''),
                'lastNameNative': student_info.get('lastNameNative', ''),
                'idCard': student_info.get('idCard', ''),
                'gender': student_info.get('gender', ''),
                'dob': student_info.get('dob'),
                'campusId': student_info.get('campusId')
            }
        
        if not transcript_by_student_structure[key]['structureInfo']:
            transcript_by_student_structure[key]['structureInfo'] = {
                'structureRecordId': structure_record_id,
                'name': structure_info.get('name', ''),
                'groupStructureId': structure_info.get('groupStructureId'),
                'academicYear': structure_info.get('academicYear', '')
            }
        
        # Add this subject to the list
        transcript_by_student_structure[key]['subjectDetails'].append(subject_detail)
        
        # Update totals - but only if the subject has valid credit and GPA
        if subject_credit > 0:
            transcript_by_student_structure[key]['totalCredits'] += subject_credit
            transcript_by_student_structure[key]['weightedGpaSum'] += (subject_credit * gpa)
        
        # Update scorer and marked at if needed
        if not transcript_by_student_structure[key]['scorerId'] and scores_list[0].get('scorerId'):
            transcript_by_student_structure[key]['scorerId'] = scores_list[0]['scorerId']
            
        if not transcript_by_student_structure[key]['markedAt'] and scores_list[0].get('markedAt'):
            transcript_by_student_structure[key]['markedAt'] = format_datetime(scores_list[0]['markedAt'])
    
    # 5. Convert to final transcript records
    transcript_records = []
    
    for (student_id, structure_record_id), transcript_data in transcript_by_student_structure.items():
        # Skip records with no subjects
        if not transcript_data['subjectDetails']:
            continue
            
        # Calculate total GPA
        total_gpa = 0.0
        if transcript_data['totalCredits'] > 0:
            total_gpa = transcript_data['weightedGpaSum'] / transcript_data['totalCredits']
        
        # Get the first subject's school ID (all subjects in a structure should be in the same school)
        school_id = None
        for subject_detail in transcript_data['subjectDetails']:
            subject_id = subject_detail[0]
            if subject_id in subject_evaluations:
                school_id = subject_evaluations[subject_id].get('schoolId')
                if school_id:
                    break
        
        # Create the record
        record = {
            # School & Campus info
            'schoolId': school_id,
            'campusId': transcript_data['studentInfo'].get('campusId'),
            
            # Structure / Class Info
            'structureRecordId': structure_record_id,
            'structureRecordName': transcript_data['structureInfo'].get('name', ''),
            'groupStructureId': transcript_data['structureInfo'].get('groupStructureId'),
            'structurePath': f"#{structure_record_id}",
            
            # Student Info
            'studentId': student_id,
            'studentFirstName': transcript_data['studentInfo'].get('firstName', ''),
            'studentLastName': transcript_data['studentInfo'].get('lastName', ''),
            'studentFirstNameNative': transcript_data['studentInfo'].get('firstNameNative', ''),
            'studentLastNameNative': transcript_data['studentInfo'].get('lastNameNative', ''),
            'idCard': transcript_data['studentInfo'].get('idCard', ''),
            'gender': transcript_data['studentInfo'].get('gender', ''),
            'dob': transcript_data['studentInfo'].get('dob'),
            
            # Subject Details (array of tuples)
            'subjectDetails': transcript_data['subjectDetails'],
            
            # Totals
            'totalCredits': transcript_data['totalCredits'],
            'totalGPA': total_gpa,
            'subjectCount': len(transcript_data['subjectDetails']),
            
            # Additional Info
            'scorerId': transcript_data['scorerId'],
            'markedAt': transcript_data['markedAt'],
            
            # Timestamp
            'createdAt': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        transcript_records.append(record)
    
    # Log results
    logger.info(f"Generated {len(transcript_records)} transcript records")
    
    # Pass transformed data to the next task
    kwargs['ti'].xcom_push(key='transformed_data', value=transcript_records)

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
            return 'NULL'
        
        # Special handling for arrays of tuples (subjectDetails)
        if key == 'subjectDetails':
            formatted_tuples = []
            for tup in value:
                formatted_elements = []
                for i, elem in enumerate(tup):
                    if elem is None:
                        formatted_elements.append('NULL')  # Replace None inside tuples with NULL
                    elif i == 0:  # UUID
                        formatted_elements.append(f"'{elem}'")
                    elif isinstance(elem, str):
                        escaped = elem.replace("'", "''")
                        formatted_elements.append(f"'{escaped}'")
                    else:
                        formatted_elements.append(str(elem))
                
                formatted_tuples.append(f"({','.join(formatted_elements)})")
            
            return f"[{','.join(formatted_tuples)}]"
        
        # Format other types
        elif isinstance(value, str):
            escaped_value = value.replace("'", "''")
            return f"'{escaped_value}'"
        else:
            return str(value)
    
    # Build the formatted row values
    formatted_rows = []
    table_keys = list(data[0].keys())
    
    for row in data:
        formatted_values = [format_value(row[key], key) for key in table_keys]
        formatted_rows.append(f"({','.join(formatted_values)})")
    
    # Construct the query
    query = f'INSERT INTO clickhouse.student_transcript_staging ({",".join(table_keys)}) VALUES '
    rows_joined = ",".join(formatted_rows)
    query += rows_joined
    
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
        
        logger.info(f"Successfully loaded {len(data)} records into student_transcript_staging table")
    except Exception as e:
        logger.error(f"Error loading data to ClickHouse: {str(e)}")
        raise

# Define the DAG
dag = DAG(
    'student_transcript_etl_v2',
    default_args=default_args,
    description='Extract score data, transform it into student transcripts with hierarchy handling, and load into ClickHouse',
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
extract_task_mongo >> extract_task_postgres >> transform_task >> load_task