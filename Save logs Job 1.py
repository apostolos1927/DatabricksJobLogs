# Databricks notebook source
from pyspark.sql.types import (
    IntegerType,
    StructField,
    StructType,
    TimestampType,
    StringType
)
from pyspark.dbutils import DBUtils
import json
from delta import DeltaTable
import datetime as dt
from datetime import datetime


def save_jobs_log(log_data, job_log_dir):
    job_schema = StructType(
        [
            StructField("job_log_id", StringType()),
            StructField("run_id", StringType()),
            StructField("job_name", StringType()),
            StructField("notebookId", StringType()),
            StructField("user", StringType()),
            StructField("clusterId", StringType()),
            StructField("jobParametersCount", StringType()),
            StructField("startTimestamp", StringType()),
            StructField("taskKey", StringType()),
            StructField("operation", StringType()),
            StructField("target_table", StringType()),
            StructField("updated_rows", IntegerType()),
            StructField("processed_ts", TimestampType()),
        ]
    )
    if not DeltaTable.isDeltaTable(spark, job_log_dir):
        df = spark.createDataFrame([], schema=job_schema)
        df.write.format("delta").option("overwriteSchema", "True").mode("append").save(job_log_dir)

    df = spark.createDataFrame(log_data, schema=job_schema)
    df.write.format("delta").mode("append").save(job_log_dir)


def store_job_logs(df, operation, target_table, job_log_dir):
    dbutils = DBUtils(spark)
    run_params = (
        dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
    )
    run_params_json = json.loads(run_params)
    print("run_params_json", json.dumps(run_params_json, indent=4))
    log_data = [
        {
            "job_log_id": run_params_json["tags"]["jobId"],
            "run_id": run_params_json["currentRunId"]["id"],
            "job_name": run_params_json["tags"]["jobName"],
            "notebookId": run_params_json["tags"]["notebookId"],
            "user": run_params_json["tags"]["user"],
            "clusterId": run_params_json["tags"]["clusterId"],
            "jobParametersCount": run_params_json["tags"]["jobParametersCount"],
            "startTimestamp": datetime.utcfromtimestamp(int(run_params_json["tags"]["startTimestamp"])/1000).strftime('%Y-%m-%d %H:%M:%S'),
            "taskKey": run_params_json["tags"]["taskKey"],
            "operation": operation,
            "target_table": target_table,
            "updated_rows": df.count(),
            "processed_ts": dt.datetime.now(),
        }
    ]
    save_jobs_log(log_data, job_log_dir)


def do_stuff(df, target_table):
    """Place your logic"""
    df.write.mode("overwrite").saveAsTable(target_table)


if __name__ == "__main__":
    dept = [
        (1, "Finance", 10, 20240429),
        (2, "Marketing", 20, 20240429),
        (3, "Sales", 30, 20240429),
        (4, "IT", 40, 20240426),
        (5, "Accounting", 33, 20240425),
        (5, "Accounting", 34, 20240423),
        (5, "Accounting", 36, 20240422),
        (5, "Accounting", 37, 20240421),
    ]
    rdd = sc.parallelize(dept)
    columns = ["ID", "DEPARTMENT", "VALUE", "DATEKEY"]
    df = rdd.toDF(columns)
    target_table = "department"
    do_stuff(df, target_table)
 
    operation = "department_store_details"
    job_log_dir = "/mnt/demo/job_logs/"
    store_job_logs(df, operation, target_table, job_log_dir)
