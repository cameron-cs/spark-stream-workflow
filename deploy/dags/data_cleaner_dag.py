from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.macros import ds_add

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(dag_id='data_workflow_cleaner_job_dag',
          default_args=default_args,
          start_date=datetime(2024, 3, 10),
          schedule_interval='0 7 * * *',
          catchup=False
          )


airflow_blogs_workflow_delta_data_cleaner_var = Variable.get("blogs_workflow_delta_data_cleaner_params", deserialize_json=True)
hdfs_blogs_workflow_delta_data_path = airflow_blogs_workflow_delta_data_cleaner_var['hdfsBlogsPath']
blogs_workflow_delta_data_cleaner_name = airflow_blogs_workflow_delta_data_cleaner_var['cleanerDataName']
blogs_workflow_delta_data_cleaner_skip_trash = airflow_blogs_workflow_delta_data_cleaner_var['skipTrash']

airflow_blogs_workflow_data_cleaner_var = Variable.get("blogs_workflow_data_cleaner_params", deserialize_json=True)
hdfs_blogs_workflow_data_path = airflow_blogs_workflow_data_cleaner_var['hdfsBlogsPath']
blogs_data_workflow_cleaner_name = airflow_blogs_workflow_data_cleaner_var['cleanerDataName']
blogs_data_workflow_cleaner_skip_trash = airflow_blogs_workflow_data_cleaner_var['skipTrash']

airflow_metrics_workflow_delta_data_cleaner_var = Variable.get("metrics_workflow_delta_data_cleaner_params", deserialize_json=True)
hdfs_metrics_workflow_delta_data_path = airflow_metrics_workflow_delta_data_cleaner_var['hdfsBlogsPath']
metrics_delta_workflow_data_cleaner_name = airflow_metrics_workflow_delta_data_cleaner_var['cleanerDataName']
metrics_delta_workflow_data_cleaner_skip_trash = airflow_metrics_workflow_delta_data_cleaner_var['skipTrash']

airflow_posts_workflow_delta_data_cleaner_var = Variable.get("posts_workflow_delta_data_cleaner_params", deserialize_json=True)
hdfs_posts_workflow_delta_data_path = airflow_posts_workflow_delta_data_cleaner_var['hdfsBlogsPath']
posts_delta_workflow_data_cleaner_name = airflow_posts_workflow_delta_data_cleaner_var['cleanerDataName']
posts_delta_workflow_data_cleaner_skip_trash = airflow_posts_workflow_delta_data_cleaner_var['skipTrash']
self_only = True

upper_bound = ''
lower_bound = ''

marker_alias = 'data_workflow_cleaner_marker'
trigger_dag_alias = 'data_workflow_cleaner_trigger_dag'

def set_conf_init(**kwargs):
    if 'dag_run' in kwargs and kwargs['dag_run'].conf:
        conf = kwargs['dag_run'].conf
        upper_bound = conf.get('upper_bound', None)
        lower_bound = conf.get('lower_bound', None)
        _self_only = conf.get('self_only', True)
    else:
        upper_bound = (kwargs['execution_date'] - timedelta(days=1)).strftime("%Y-%m-%d")
        lower_bound = datetime.strptime(ds_add(kwargs['ds'], -10), "%Y-%m-%d").strftime("%Y-%m-%d")
        _self_only = self_only

    return {
        'upper_bound': upper_bound,
        'lower_bound': lower_bound,
        'self_only': _self_only
    }


def _check_self_only(**context):
    _self_only = True
    if 'dag_run' in context and context['dag_run'].conf:
        conf = context['dag_run'].conf
        _self_only = conf.get('self_only', True)
    return marker_alias if _self_only else trigger_dag_alias

start = PythonOperator(
    task_id='start_workflow_data_cleaner_job',
    python_callable=set_conf_init,
    provide_context=True,
    dag=dag,
    trigger_rule='none_failed'
)

spark_blogs_workflow_delta_data_cleaner_job = SparkSubmitOperator(
    task_id='spark_blogs_workflow_delta_data_cleaner_job',
    yarn_queue='default',
    java_class = 'org.cameron.cs.BlogsDeltaCleanerApp',
    application='/usr/local/airflow/spark/data_cleaner/data_cleaner.jar',
    name='spark_blogs_workflow_delta_data_cleaner_job',
    conf={
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.dynamicAllocation.enabled": "true",
        "spark.hadoop.validateOutputSpecs": "false",
        "spark.yarn.queue": "default",
        "spark.shuffle.service.enabled": "true",
        "spark.dynamicAllocation.initialExecutors": "5",
        "spark.dynamicAllocation.minExecutors": "5",
        "spark.dynamicAllocation.maxExecutors": "50",
        "spark.driver.memory": "16g",
        "spark.driver.cores": "4",
        "spark.executor.cores": "4",
        "spark.executor.memory": "16g"
    },
    verbose=False,
    retries=0,
    provide_context=True,
    application_args=[
        '-u', "{{ task_instance.xcom_pull(task_ids='start_workflow_data_cleaner_job')['upper_bound'] }}",
        '-l', "{{ task_instance.xcom_pull(task_ids='start_workflow_data_cleaner_job')['lower_bound'] }}",
        '-p', hdfs_blogs_workflow_delta_data_path,
        '-n', blogs_workflow_delta_data_cleaner_name,
        '-s', str(blogs_workflow_delta_data_cleaner_skip_trash)
    ],
    dag=dag
)

spark_blogs_workflow_data_cleaner_job = SparkSubmitOperator(
    task_id='spark_blogs_workflow_data_cleaner_job',
    yarn_queue='default',
    java_class = 'org.cameron.cs.BlogsCleanerApp',
    application='/usr/local/airflow/spark/data_workflow_cleaner/data_workflow_cleaner.jar',
    name='spark_blogs_workflow_data_cleaner_job',
    conf={
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.dynamicAllocation.enabled": "true",
        "spark.hadoop.validateOutputSpecs": "false",
        "spark.yarn.queue": "default",
        "spark.shuffle.service.enabled": "true",
        "spark.dynamicAllocation.initialExecutors": "5",
        "spark.dynamicAllocation.minExecutors": "5",
        "spark.dynamicAllocation.maxExecutors": "50",
        "spark.driver.memory": "16g",
        "spark.driver.cores": "4",
        "spark.executor.cores": "4",
        "spark.executor.memory": "16g"
    },
    verbose=False,
    retries=0,
    provide_context=True,
    application_args=[
        '-u', "{{ task_instance.xcom_pull(task_ids='start_workflow_data_cleaner_job')['upper_bound'] }}",
        '-l', "{{ task_instance.xcom_pull(task_ids='start_workflow_data_cleaner_job')['lower_bound'] }}",
        '-p', hdfs_blogs_workflow_data_path,
        '-n', blogs_data_workflow_cleaner_name,
        '-s', str(blogs_data_workflow_cleaner_skip_trash)
    ],
    dag=dag
)


spark_metrics_workflow_delta_data_cleaner_job = SparkSubmitOperator(
    task_id='spark_metrics_workflow_delta_data_cleaner_job',
    yarn_queue='default',
    java_class = 'org.cameron.cs.MetricsDeltaCleanerApp',
    application='/usr/local/airflow/spark/data_workflow_cleaner/data_workflow_cleaner.jar',
    name='spark_metrics_workflow_delta_data_cleaner_job',
    conf={
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.dynamicAllocation.enabled": "true",
        "spark.hadoop.validateOutputSpecs": "false",
        "spark.yarn.queue": "default",
        "spark.shuffle.service.enabled": "true",
        "spark.dynamicAllocation.initialExecutors": "5",
        "spark.dynamicAllocation.minExecutors": "5",
        "spark.dynamicAllocation.maxExecutors": "50",
        "spark.driver.memory": "16g",
        "spark.driver.cores": "4",
        "spark.executor.cores": "4",
        "spark.executor.memory": "16g"
    },
    verbose=False,
    retries=0,
    provide_context=True,
    application_args=[
        '-u', "{{ task_instance.xcom_pull(task_ids='start_workflow_data_cleaner_job')['upper_bound'] }}",
        '-l', "{{ task_instance.xcom_pull(task_ids='start_workflow_data_cleaner_job')['lower_bound'] }}",
        '-p', hdfs_metrics_workflow_delta_data_path,
        '-n', metrics_delta_workflow_data_cleaner_name,
        '-s', str(metrics_delta_workflow_data_cleaner_skip_trash)
    ],
    dag=dag
)

spark_posts_workflow_delta_data_cleaner_job = SparkSubmitOperator(
    task_id='spark_posts_workflow_delta_data_cleaner_job',
    yarn_queue='default',
    java_class = 'org.cameron.cs.PostsDeltaCleanerApp',
    application='/usr/local/airflow/spark/data_workflow_cleaner/data_workflow_cleaner.jar',
    name='spark_posts_workflow_delta_data_cleaner_job',
    conf={
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.dynamicAllocation.enabled": "true",
        "spark.hadoop.validateOutputSpecs": "false",
        "spark.yarn.queue": "default",
        "spark.shuffle.service.enabled": "true",
        "spark.dynamicAllocation.initialExecutors": "5",
        "spark.dynamicAllocation.minExecutors": "5",
        "spark.dynamicAllocation.maxExecutors": "50",
        "spark.driver.memory": "16g",
        "spark.driver.cores": "4",
        "spark.executor.cores": "4",
        "spark.executor.memory": "16g"
    },
    verbose=False,
    retries=0,
    provide_context=True,
    application_args=[
        '-u', "{{ task_instance.xcom_pull(task_ids='start_workflow_data_cleaner_job')['upper_bound'] }}",
        '-l', "{{ task_instance.xcom_pull(task_ids='start_workflow_data_cleaner_job')['lower_bound'] }}",
        '-p', hdfs_posts_workflow_delta_data_path,
        '-n', posts_delta_workflow_data_cleaner_name,
        '-s', str(posts_delta_workflow_data_cleaner_skip_trash)
    ],
    dag=dag
)

marker = DummyOperator(
    task_id=marker_alias,
    provide_context=True,
    python_callable=set_conf_init,
    trigger_rule='none_failed'
)


check_self_only = BranchPythonOperator(
    task_id='check_self_only',
    provide_context=True,
    python_callable=_check_self_only,
    trigger_rule='all_done',
)


trigger_edge_dag = DummyOperator(task_id=trigger_dag_alias, trigger_rule='none_failed')


start >> spark_blogs_workflow_delta_data_cleaner_job >> spark_metrics_workflow_delta_data_cleaner_job >> spark_posts_workflow_delta_data_cleaner_job >> spark_blogs_workflow_data_cleaner_job
[spark_blogs_workflow_delta_data_cleaner_job, spark_metrics_workflow_delta_data_cleaner_job, spark_posts_workflow_delta_data_cleaner_job, spark_blogs_workflow_data_cleaner_job, trigger_edge_dag] >> marker
[spark_blogs_workflow_delta_data_cleaner_job, spark_metrics_workflow_delta_data_cleaner_job, spark_posts_workflow_delta_data_cleaner_job, spark_blogs_workflow_data_cleaner_job] >> check_self_only
check_self_only >> [trigger_edge_dag, marker]