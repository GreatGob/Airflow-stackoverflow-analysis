import datetime
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from google_drive_downloader import GoogleDriveDownloader as gdd

DATA_PATH = "data"
QUESTION_ID = "1pzhWKoKV3nHqmC7FLcr5SzF7qIChsy9B"
ANSWER_ID = "1FflYh-YxXDvJ6NyE9GNhnMbB-M87az0Y"
DATABASE_NAME = "stackoverflow"

default_args = {
    "owner": "airflow",
    "start_date": datetime.datetime(2024, 4, 6),
}


def check_download_files(**kwargs):
    questions_file_exists = os.path.exists(f"{DATA_PATH}/Questions.csv")
    answers_file_exists = os.path.exists(f"{DATA_PATH}/Answers.csv")
    if questions_file_exists and answers_file_exists:
        return "end"
    else:
        return "clear_files"


def download_file(file_id, dest_file):
    os.system(
        "cd && ls && sudo chown -R airflow:airflow /data && sudo chmod -R 777 /data"
    )
    gdd.download_file_from_google_drive(file_id=file_id, dest_path=dest_file)


with DAG(
    dag_id="stackoverflow",
    default_args=default_args,
    schedule_interval="@daily",
) as dag:
    start_task = DummyOperator(task_id="start")

    check_download_files_task = BranchPythonOperator(
        task_id="check_download_files",
        python_callable=check_download_files,
        dag=dag,
    )

    clear_files_task = BashOperator(
        task_id="clear_files",
        bash_command="rm -f {DATA_PATH}/Questions.csv {DATA_PATH}/Answers.csv",
        dag=dag,
    )

    download_question_file_task = PythonOperator(
        task_id="download_question_file_task",
        python_callable=download_file,
        op_args=[QUESTION_ID, f"{DATA_PATH}/Questions.csv"],
    )

    download_answer_file_task = PythonOperator(
        task_id="download_answer_file_task",
        python_callable=download_file,
        op_args=[ANSWER_ID, f"{DATA_PATH}/Answers.csv"],
    )

    import_questions_mongo = BashOperator(
        task_id="import_questions_mongo",
        bash_command=f"mongoimport --type csv -d {DATABASE_NAME} -c questions --headerline --drop {DATA_PATH}/Questions.csv",
    )

    import_answers_mongo = BashOperator(
        task_id="import_answers_mongo",
        bash_command=f"mongoimport --type csv -d {DATABASE_NAME} -c answers --headerline --drop {DATA_PATH}/Answers.csv",
    )

    spark_process = BashOperator(
        task_id="spark_process", bash_command="echo spark_process"
    )

    import_output_mongo = BashOperator(
        task_id="import_output_mongo", bash_command="echo import_output_mongo"
    )

    end_task = DummyOperator(task_id="end")

start_task >> check_download_files_task
check_download_files_task >> [clear_files_task, end_task]
clear_files_task >> [download_question_file_task, download_answer_file_task]
download_question_file_task >> import_questions_mongo
download_answer_file_task >> import_answers_mongo
(
    [import_questions_mongo, import_answers_mongo]
    >> spark_process
    >> import_output_mongo
    >> end_task
)
