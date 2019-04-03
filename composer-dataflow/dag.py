from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.contrib.operators import dataflow_operator

dataflow_default_options =  {
        'project': 'my-gcp-project',
        'region': 'europe-west1',
        'zone': 'europe-west1-d',
        'tempLocation': 'gs://my-staging-bucket/staging/',
}

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'dataflow_default_options': dataflow_default_options
}

dag = DAG(
    dag_id='example_dag',
    default_args=args,
    schedule_interval='0 2 * * *',
    dagrun_timeout=timedelta(minutes=60),
)

t1 = dataflow_operator.DataflowTemplateOperator(
    task_id='dataflow_example',
    template='gs://your_bucket/template',
    parameters={
        'bucket': "gs://your_bucket/files",
        'tableRef': "your_project:dataset.table"
    },
    gcp_conn_id='google_cloud_default',
    dag=dag
)