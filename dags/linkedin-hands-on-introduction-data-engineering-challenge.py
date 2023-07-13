""" Basic ETL DAG"""
from datetime import datetime, date
import pandas as pd
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG

with DAG(
    dag_id="challenge_dag",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    extract_task = BashOperator(
        task_id="extract_task",
        bash_command="wget -c https://datahub.io/core/s-and-p-500-companies/r/constituents.csv -O /tmp/sp_500.csv",
    )

    def transform_data():
        """Read in the file, and write a transformed file out"""
        df = (
            pd.read_csv("/tmp/sp_500.csv", usecols=["Sector"])
            .value_counts()
            .rename("Count")
            .to_frame()
        )
        df["Date"] = date.today().strftime("%Y-%m-%d")
        df.to_csv("/tmp/sp_500_sector_count.csv", index=True)

    transform_task = PythonOperator(
        task_id="transform_task", python_callable=transform_data, dag=dag
    )

    # load_task = BashOperator(
    #     task_id='load_task',
    #     bash_command='echo -e ".separator ","\n.import --skip 1 /workspaces/hands-on-introduction-data-engineering-4395021/lab/challenge/sp_500_sector_count.csv sp_500_sector_count" | sqlite3 /workspaces/hands-on-introduction-data-engineering-4395021/lab/challenge/challenge-load-db.db',
    #     dag=dag)

    # extract_task >> transform_task >> load_task
    extract_task >> transform_task
