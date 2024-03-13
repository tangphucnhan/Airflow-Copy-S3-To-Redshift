"""
If you have your own local folder structure, and you put Airflow code somewhere.
Run below command in terminal to link the dag file to 'dags' folder of AIRFLOW_PATH, then Airflow will load dag.
Note: use absolute path
-F to force to overwrite

ln -Fs /User/path/to/dag_1.py /User/airflow/path/dags/
"""

import os
import sys
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from dotenv import load_dotenv
import boto3
from prettyprinter import pformat


"""
In a use case that airflow source can not detect the root path, then it can not import code from another files.
This snippet will add project path to the system path.
"""
prj_path = os.path.realpath(__file__).split("/")
prj_path.pop()
prj_path.pop()
prj_path = "/".join(prj_path)
sys.path.append(prj_path)
load_dotenv(prj_path + "/.env")
from db.drivers_config import *
import db.sql_scripts as sql_scripts


is_multi_files = True
csv_file = "redshift_bill_001.csv"
manifest_file = "daily.manifest"


def prepare_s3_manifest_file(s3_manifest_file_key, **kwargs):
    lakehouse_s3 = cfg_lakehouse[CFG_S3]
    s3_client = boto3.client(
        "s3",
        region_name=lakehouse_s3["aws_region_name"],
        aws_access_key_id=lakehouse_s3["aws_access_key_id"],
        aws_secret_access_key=lakehouse_s3["aws_secret_access_key"],
        endpoint_url=lakehouse_s3["aws_endpoint_url"],
    )
    files: dict = s3_client.list_objects_v2(
        Bucket=lakehouse_s3["s3_bucket"],
        Prefix=lakehouse_s3["s3_prefix"],
    )
    file_list = [{"url": f"s3://{lakehouse_s3['s3_bucket']}/{file['Key']}"} for file in files["Contents"]]
    entries = {
        "entries": file_list
    }
    s3_client.put_object(
        Body=pformat(entries),
        Bucket=lakehouse_s3["s3_bucket"],
        Key=s3_manifest_file_key,
    )


def load_s3_to_redshift(s3_file_key, manifest_text, **kwargs):
    lakehouse_s3 = cfg_lakehouse[CFG_S3]
    warehouse_rs = cfg_warehouse[CFG_REDSHIFT]
    s3_file = f"s3://{lakehouse_s3['s3_bucket']}/{s3_file_key}"
    redshift_client = boto3.client(
        warehouse_rs['db_conn_id'],
        region_name=warehouse_rs['db_region'],
        aws_access_key_id=warehouse_rs['db_access_key'],
        aws_secret_access_key=warehouse_rs['db_secret_key'],
    )
    redshift_client.execute_statement(
        Database=warehouse_rs['db_name'],
        WorkgroupName=warehouse_rs['db_workgroup'],
        Sql=sql_scripts.CREATE_TABLE,
    )
    redshift_client.execute_statement(
        Database=warehouse_rs['db_name'],
        WorkgroupName=warehouse_rs['db_workgroup'],
        Sql=sql_scripts.COPY_S3_TO_REDSHIFT.format(
            s3_file,
            lakehouse_s3["aws_iam_role"],
            warehouse_rs['db_region'],
            manifest_text
        ),
    )


with DAG(
    dag_id="data_bloom",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 3, 12, tz="UTC"),
    catchup=False,
    tags=["data_bloom"]
) as dag:
    file_key = manifest_file if is_multi_files else csv_file
    str_manifest = "manifest" if is_multi_files else ""

    t_s3_to_redshift = PythonOperator(
        task_id="load_s3_to_redshift",
        python_callable=load_s3_to_redshift,
        op_kwargs={
            "s3_file_key": file_key,
            "manifest_text": str_manifest,
        }
    )

    t_teardown = BashOperator(
        task_id="teardown",
        bash_command="echo '!!!!!!!!!!!! Teardown'",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    if is_multi_files:
        t_prepare_manifest = PythonOperator(
            task_id="prepare_s3_manifest",
            python_callable=prepare_s3_manifest_file,
            op_kwargs={"s3_manifest_file_key": manifest_file}
        )
        t_prepare_manifest >> t_s3_to_redshift >> t_teardown
    else:
        t_s3_to_redshift >> t_teardown
