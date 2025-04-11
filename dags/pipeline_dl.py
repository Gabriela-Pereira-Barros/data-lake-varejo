from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import time

# Default arguments
default_args = {
    'owner': 'Gabriela',
    'email': 'gabrielapbarros15@gmail.com',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


# AWS Client Function
def get_aws_client(service):
    try:
        return boto3.client(service, region_name='us-east-1')
    except Exception as e:
        raise Exception(f'Erro ao conectar com AWS {service}! Error: {e}')


# EMR Cluster Management
def create_emr_cluster(ti):
    emr_client = get_aws_client('emr')
    response = emr_client.run_job_flow(
        Name='Cluster Data Lake PoD Academy',
        LogUri='s3://data-lake-varejo-905418122144/08_logs/',
        ReleaseLabel='emr-7.3.0',
        Instances={
            'InstanceGroups': [{
                'Name': 'Master',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            }],
            'KeepJobFlowAliveWhenNoSteps': True
        },
        Applications=[{'Name': 'Spark'}],
        BootstrapActions=[{
            'Name': 'CustomBootstrapAction',
            'ScriptBootstrapAction': {
                'Path': 's3://data-lake-varejo-905418122144/07_config/bootstrap.sh'
            }
        }],
        ServiceRole='EMR_DefaultRole',
        JobFlowRole='EMR_EC2_DefaultRole'
    )
    cluster_id = response['JobFlowId']
    print(f'Cluster criado! ID: {cluster_id}')
    ti.xcom_push(key='cluster_id', value=cluster_id)

def terminate_emr_cluster(ti):
    cluster_id = ti.xcom_pull(key='cluster_id')
    get_aws_client('emr').terminate_job_flows(JobFlowIds=[cluster_id])
    print('Cluster terminado!')


# Step Job Management
def add_step_job(ti, step_name, script_path):
    cluster_id = ti.xcom_pull(key='cluster_id')
    emr_client = get_aws_client('emr')
    
    step = {
        'Name': step_name,
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '--deploy-mode', 'client', script_path]
        }
    }
    
    response = emr_client.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step])
    step_id = response['StepIds'][0]
    print(f'{step_name} adicionado ao cluster! STEP_JOB_ID: {step_id}')
    ti.xcom_push(key='step_job_id', value=step_id)

def wait_step_job(ti):
    emr_client = get_aws_client('emr')
    cluster_id = ti.xcom_pull(key='cluster_id')
    step_id = ti.xcom_pull(key='step_job_id')
    
    while True:
        state = emr_client.describe_step(ClusterId=cluster_id, StepId=step_id)['Step']['Status']['State']
        if state in ['PENDING', 'RUNNING', 'CANCELLED']:
            print(f'Executando step job... Estado: {state}')
            time.sleep(10)
        elif state == 'COMPLETED':
            print(f'Step job finalizado... Estado: {state}')
            break
        else:
            raise Exception(f'Error! Estado: {state}')


# Task Factory
def create_step_tasks(step_name, script_path):
    base_path = 's3://data-lake-varejo-905418122144/06_codes/scripts/'
    add_task = PythonOperator(
        task_id=f'add_{step_name}',
        python_callable=add_step_job,
        op_kwargs={'step_name': step_name, 'script_path': f'{base_path}{script_path}'}
    )
    wait_task = PythonOperator(
        task_id=f'wait_{step_name}',
        python_callable=wait_step_job
    )
    return add_task, wait_task


# DAG Definition
with DAG(
    'pipeline_dl',
    tags=['podacademy'],
    start_date=datetime(2025, 4, 11),
    default_args=default_args,
    schedule_interval='@monthly',
    catchup=False
) as dag:
    
    # Initial Cluster Creation
    start = PythonOperator(
        task_id='create_emr_cluster',
        python_callable=create_emr_cluster
    )
    
    # Step Definitions
    steps = [
        ('prgm_raw', 'prgm_raw.py'),
        ('prgm_trusted_customers', 'prgm_trusted_customers.py'),
        ('prgm_trusted_discounts', 'prgm_trusted_discounts.py'),
        ('prgm_trusted_employees', 'prgm_trusted_employees.py'),
        ('prgm_trusted_products', 'prgm_trusted_products.py'),
        ('prgm_trusted_stores', 'prgm_trusted_stores.py'),
        ('prgm_trusted_transactions', 'prgm_trusted_transactions.py'),
        ('prgm_refined_data_viz', 'prgm_refined_data_viz.py'),
        ('prgm_refined_book', 'prgm_refined_book.py')
    ]
    
    # Create all tasks
    task_pairs = [create_step_tasks(name, script) for name, script in steps]
    
    # Final Cluster Termination
    end = PythonOperator(
        task_id='terminate_emr_cluster',
        python_callable=terminate_emr_cluster
    )
    
    # Set Dependencies
    previous_task = start
    for add_task, wait_task in task_pairs:
        previous_task >> add_task >> wait_task
        previous_task = wait_task
    previous_task >> end