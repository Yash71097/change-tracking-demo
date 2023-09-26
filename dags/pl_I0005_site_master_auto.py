import os,requests,json
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.utils.timezone import datetime
from airflow import DAG
from airflow.decorators import dag, task_group
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator, DatabricksSubmitRunOperator
from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow import Dataset  
from astro_databricks import DatabricksNotebookOperator, DatabricksWorkflowTaskGroup
#%%
"""Default Arguments"""
databricks_conn_id="databricks_default"
azure_data_factory_conn_id="adf_perf_conn"

default_init_framework="dbfs:/databricks/init/scripts/framework_v3.sh"
default_init_master="dbfs:/databricks/init/scripts/master_init.sh"

default_cluster_version="11.3.x-scala2.12"
cluster_version113="11.3.x-scala2.12"
cluster_version91="9.1.x-scala2.12"

dag_id="pl_I0005_site_master_auto"
schedule = '05 23  *  *  *'
catchup=False

#%%
"""Cluster Definition"""

cluster_spec = [
        {
        "job_cluster_key": f"{dag_id}-job-cluster",
        "new_cluster": {
            "spark_version": "9.1.x-scala2.12",
            "node_type_id": "Standard_D8s_v3",
            "spark_env_vars": {"PYSPARK_PYTHON": "/databricks/python3/bin/python3"},
            "num_workers":"1",
            "custom_tags": {"DBUClusterName":f"{dag_id}-cluster1"},
            "cluster_log_conf": {"dbfs" : { "destination" : "dbfs:/mnt/cluster-logs"}},
            "init_scripts": [   {"dbfs" : { "destination" : f"{default_init_framework}"}},
                                {"dbfs" : { "destination" : f"{default_init_master}"}},
                            ],
                    }
        }
                    ]


#%%
"""DAG Definition"""

with DAG(
    dag_id="pl_I0005_site_master_auto",
    start_date=days_ago(1),
    schedule = '05 23  *  *  *',
    catchup=False,
    default_args = {
    "owner":"p9162182",
    "default_view":"graph",
    "retries": 1,
    "retry_delay": timedelta(seconds=60), 
    "azure_data_factory_conn_id" : "adf_perf_conn",
    },
    
    default_view="graph",
    ) as dag:

        with DatabricksWorkflowTaskGroup(
            group_id="dbk_automate",
            job_clusters=cluster_spec,
            databricks_conn_id=databricks_conn_id,
            notebook_packages=[{"pypi": { "package": "pandas" },
                                "pypi": { "package": "datetime" }
                                }],
            notebook_params={"pipeline": "pl_I0005_site_master_auto",}
            ) as ddl_workflow:


            notebook_1 = DatabricksNotebookOperator(
                job_cluster_key=f"{dag_id}-job-cluster",
                task_id="dml_job1",
                notebook_path="/projects/i0005_site/i0005_site_master",
                notebook_params={'day_diff':'2','feed_name':'I0005_site','pipeline_id':f"{dag_id}-datetime.now()",'job_config_path':'/dbfs/mnt/projects/i0005_site/configs/i0005_site_master_kafka_config.yaml','pipeline_name':'pl_I0005_site_master_auto'},
                databricks_conn_id=databricks_conn_id,
                source="WORKSPACE",
                outlets = [Dataset("sap_ingest_ndev.site_Layout_ns_tbl"),Dataset("sap_ingest_ndev.site_contacts_ns_tbl"),Dataset("sap_ingest_ndev.site_master_ns_tbl"),Dataset("sap_ingest_ndev.site_articlegroup_ns_tbl")],
                )