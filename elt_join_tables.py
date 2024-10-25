from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

# Define DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='elt_join_tables',
    default_args=default_args,
    description='An ELT DAG to join session_summary and user_data with deduplication',
    schedule_interval='@daily',
    start_date=datetime(2024, 10, 23),
    catchup=False,
) as dag:

    def extract_and_transform():
        """Extract data from both tables and apply transformations."""
        postgres_hook = PostgresHook(postgres_conn_id='airflow_db')
        query = """
            SELECT
                ss.session_id,
                ss.user_id,
                ss.session_duration,
                ud.user_name,
                ud.email
            FROM analytics.session_summary AS ss
            JOIN public.user_data AS ud
            ON ss.user_id = ud.user_id;
        """
        records = postgres_hook.get_records(query)
        # Returning records to be used in the next task
        return records

    def load_into_joined_table(ti):
        """Load the transformed data into the joined table with deduplication."""
        postgres_hook = PostgresHook(postgres_conn_id='airflow_db')
        records = ti.xcom_pull(task_ids='extract_and_transform')
        
        # Create JOINED table if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS analytics.joined_table (
            session_id INT,
            user_id INT,
            session_duration INT,
            user_name VARCHAR(255),
            email VARCHAR(255),
            PRIMARY KEY (session_id, user_id)
        );
        """
        postgres_hook.run(create_table_query)

        # Insert records with duplicate check
        insert_query = """
        INSERT INTO analytics.joined_table (session_id, user_id, session_duration, user_name, email)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (session_id, user_id) DO NOTHING;
        """
        for record in records:
            postgres_hook.run(insert_query, parameters=record)

    # Define tasks
    extract_and_transform_task = PythonOperator(
        task_id='extract_and_transform',
        python_callable=extract_and_transform
    )

    load_task = PythonOperator(
        task_id='load_into_joined_table',
        python_callable=load_into_joined_table
    )

    # Task dependencies
    extract_and_transform_task >> load_task
