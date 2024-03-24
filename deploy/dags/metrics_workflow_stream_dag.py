from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(dag_id='metrics_workflow_stream_job_dag',
          default_args=default_args,
          start_date=datetime(2024, 3, 8),
          schedule_interval='0 * * * *',
          catchup=False
          )

airflow_var = Variable.get("metrics_workflow_stream_params", deserialize_json=True)

kafka_host = airflow_var['kafkaHost']
kafka_consumer_group = airflow_var['kafkaConsumerGroup']
metrics_topic = airflow_var['metricsTopicName']
hdfs_path = airflow_var['hdfsPath']
hdfs_offsets_path = airflow_var['hdfsOffsetsPath']

spark_job = SparkSubmitOperator(
    task_id='metrics_workflow_stream_job',
    jars='/usr/lib/spark/jars/spark-sql-kafka-0-10_2.11-2.4.4.jar',
    yarn_queue='zeppelin',
    java_class = 'org.cameron.cs.MetricsWorkflowStreamApp',
    application='/usr/local/airflow/spark/metrics_workflow_stream/metrics_workflow_stream.jar',
    name='metrics_workflow_stream_job',
    application_args=[
        '-d', '{{ ds }}',
        '-h', kafka_host,
        '-g', kafka_consumer_group,
        '-t', metrics_topic,
        '-p', hdfs_path,
        '-o', hdfs_offsets_path
    ],
    conf={
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.dynamicAllocation.enabled": "true",
        "spark.hadoop.validateOutputSpecs": "false",
        "spark.yarn.queue": "zeppelin",
        "spark.shuffle.service.enabled": "true",
        "spark.dynamicAllocation.initialExecutors": "2",
        "spark.dynamicAllocation.minExecutors": "1",
        "spark.dynamicAllocation.maxExecutors": "20",
        "spark.driver.memory": "8g",
        "spark.driver.cores": "2",
        "spark.executor.cores": "2",
        "spark.executor.memory": "8g"
    },
    dag=dag
)

start = DummyOperator(
    task_id='start_metrics_workflow_stream_job',
    trigger_rule='none_failed'
)

end = DummyOperator(
    task_id='end_metrics_workflow_stream_job',
    trigger_rule='none_failed'
)

start >> spark_job >> end
