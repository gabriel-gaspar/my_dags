from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'gaspar',
    'start_date': datetime(2021, 9, 11)
}

with DAG(
    'spark-pi-job',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    tags=['example'],
) as dag:


    t1 = BashOperator(
        task_id='spark_pi_job',
        bash_command="""/opt/airflow/spark-3.1.2-bin-hadoop3.2/bin/spark-submit \
                        --class org.apache.spark.examples.SparkPi \
                        --master k8s://kubernetes.default.svc \
                        --deploy-mode cluster \
                        --driver-cores 1 \
                        --driver-memory 512m --executor-cores 1 \
                        --executor-memory 512m --num-executors 1 \
                        --conf spark.kubernetes.namespace=spark \
                        --conf spark.kubernetes.container.image=gcr.io/spark-operator/spark:v3.1.1 \
                        --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-operator-spark \
                        --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent \
                        --conf spark.kubernetes.driver.podTemplateFile=/opt/airflow/job_templates/basic-template.yaml \
                        --conf spark.kubernetes.executor.podTemplateFile=/opt/airflow/job_templates/basic-template.yaml \
                        local:///opt/spark/examples/jars/spark-examples_2.12-3.1.1.jar 1000""",
    )

    t2 = BashOperator(
        task_id="end_message",
        bash_command='echo "The task is done!"'
    )

    t1 >> t2