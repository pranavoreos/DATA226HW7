from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 1),
}

with DAG('wau_etl_dag', default_args=default_args, schedule_interval='@weekly', catchup=False) as dag:

    import_user_session_channel = SnowflakeOperator(
        task_id='import_user_session_channel',
        snowflake_conn_id='snowflake_conn',  # Change if necessary
        sql='SELECT * FROM finance_project_db.analytics.user_session_channel;',
        warehouse='compute_wh',
        database='finance_project_db',
        schema='analytics',
        autocommit=True,
    )

    import_session_timestamp = SnowflakeOperator(
        task_id='import_session_timestamp',
        snowflake_conn_id='snowflake_conn',  # Change if necessary
        sql='SELECT * FROM finance_project_db.analytics.session_timestamp;',
        warehouse='compute_wh',
        database='finance_project_db',
        schema='analytics',
        autocommit=True,
    )

    # Define task dependencies
    import_user_session_channel >> import_session_timestamp

