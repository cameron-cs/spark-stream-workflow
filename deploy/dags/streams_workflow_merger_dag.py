from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.macros import ds_add

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(dag_id='streams_workflow_merger_job_dag',
          default_args=default_args,
          start_date=datetime(2024, 3, 8),
          schedule_interval='0 7 * * *',
          catchup=False
          )

airflow_var = Variable.get("streams_workflow_merger_params", deserialize_json=True)

hdfs_posts = airflow_var['hdfsPostsPath']
hdfs_offsets_posts = airflow_var['hdfsOffsetsPostsPath']
hdfs_blogs = airflow_var['hdfsBlogsPath']
hdfs_offsets_blogs = airflow_var['hdfsOffsetsBlogsPath']
hdfs_metrics = airflow_var['hdfsMetricsPath']
hdfs_offsets_metrics = airflow_var['hdfsOffsetsMetricsPath']
hdfs_merged_blogs = airflow_var['hdfsMergedBlogsPath']
hdfs_merged_posts = airflow_var['hdfsMergedPostsPath']
skip_trash = airflow_var['hdfsSkipTrash']
self_only = airflow_var['selfOnly']
edge_name = airflow_var['edgeName']
batch_size = airflow_var['batchSize']

exec_date = ''
prev_exec_date = ''
lower_bound = ''

marker_alias = 'streams_workflow_merger_marker'
trigger_dag_alias = 'streams_workflow_merger_trigger_dag'


def set_conf_init(**kwargs):
    if 'dag_run' in kwargs and kwargs['dag_run'].conf:
        conf = kwargs['dag_run'].conf
        exec_date = conf.get('exec_date', None)
        prev_exec_date = conf.get('prev_exec_date', None)
        lower_bound = conf.get('lower_bound', None)
        _self_only = conf.get('self_only', True)
    else:
        exec_date = (kwargs['execution_date'] - timedelta(days=1)).strftime("%Y-%m-%d")
        prev_exec_date = kwargs['prev_execution_date'].strftime("%Y-%m-%d")
        lower_bound = datetime.strptime(ds_add(kwargs['ds'], -30), "%Y-%m-%d").strftime("%Y-%m-%d")
        _self_only = self_only

    return {
        'exec_date': exec_date,
        'prev_exec_date': prev_exec_date,
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
    task_id='start_streams_workflow_merger_job',
    python_callable=set_conf_init,
    provide_context=True,
    dag=dag,
    trigger_rule='none_failed'
)


spark_blogs_workflow_job = SparkSubmitOperator(
    task_id='streams_blogs_workflow_merger_job',
    yarn_queue='default',
    java_class = 'org.cameron.cs.BlogsStreamsMergerApp',
    application='/usr/local/airflow/spark/streams_workflow_merger/streams_workflow_merger.jar',
    name='streams_blogs_workflow_merger_job',
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
        '-d', "{{ task_instance.xcom_pull(task_ids='start_streams_workflow_merger_job')['exec_date'] }}",
        '--prevExecDate', "{{ task_instance.xcom_pull(task_ids='start_streams_workflow_merger_job')['prev_exec_date'] }}",
        '--lowerBound', "{{ task_instance.xcom_pull(task_ids='start_streams_workflow_merger_job')['lower_bound'] }}",
        '-p', hdfs_posts,
        '--po', hdfs_offsets_posts,
        '-b', hdfs_blogs,
        '--bo', hdfs_offsets_blogs,
        '-m', hdfs_metrics,
        '--mo', hdfs_offsets_metrics,
        '--mb', hdfs_merged_blogs,
        '--mp', hdfs_merged_posts,
        '--skipTrash', str(skip_trash)
    ],
    dag=dag
)


spark_posts_workflow_job = SparkSubmitOperator(
    task_id='streams_posts_workflow_merger_job',
    yarn_queue='default',
    java_class = 'org.cameron.cs.PostsStreamsMergerApp',
    application='/usr/local/airflow/spark/streams_workflow_merger/streams_workflow_merger.jar',
    name='streams_posts_workflow_merger_job',
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
        '-d', "{{ task_instance.xcom_pull(task_ids='start_streams_workflow_merger_job')['exec_date'] }}",
        '--prevExecDate', "{{ task_instance.xcom_pull(task_ids='start_streams_workflow_merger_job')['prev_exec_date'] }}",
        '--lowerBound', "{{ task_instance.xcom_pull(task_ids='start_streams_workflow_merger_job')['lower_bound'] }}",
        '-p', hdfs_posts,
        '--po', hdfs_offsets_posts,
        '-b', hdfs_blogs,
        '--bo', hdfs_offsets_blogs,
        '-m', hdfs_metrics,
        '--mo', hdfs_offsets_metrics,
        '--mb', hdfs_merged_blogs,
        '--mp', hdfs_merged_posts,
        '--skipTrash', str(skip_trash),
        "--batchSize", str(batch_size)
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


trigger_edge_dag = DummyOperator(task_id=trigger_dag_alias, trigger_rule='none_failed') if edge_name is None else TriggerDagRunOperator(
    task_id=trigger_dag_alias,
    trigger_dag_id='...',
    python_callable=set_conf_init,
)


start >> spark_blogs_workflow_job >> spark_posts_workflow_job
[spark_blogs_workflow_job, spark_posts_workflow_job, trigger_edge_dag] >> marker
[spark_blogs_workflow_job, spark_posts_workflow_job] >> check_self_only
check_self_only >> [trigger_edge_dag, marker]