from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
import datetime

# Script with the SQL statements used for this pipeline
import sql_statements


# DAG uses this default_args dict
default_args = {
    'owner': 'udacity',
    'start_date': datetime.datetime.utcnow(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup_by_default': False,
    'max_active_runs': 1
}


# Dag run once an hour and the DAG object has default args set
dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )


# DAG begins with a start_execution task
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


# Tasks for the stage tables

# Task for the Stage_events table
stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",   
    s3_bucket="udacity-dend",
    s3_key="log_data/"
)

# Task for the Stage_songs table
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs",
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",   
    s3_bucket="udacity-dend",
    s3_key="song-data/A/A/A/TRAAAAK128F9318786.json"
)


# Task for the fact table
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    fact_table="songplays",
    staging_events="staging_events",
    staging_songs="staging_songs"
)

# Tasks for the dimensional tables


# Task for the users table
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    dim_table="users",
    insert_sql=sql_statements.user_table_insert,
    append_data=True
)

# Task for the songs table
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    dim_table="songs",
    insert_sql=sql_statements.song_table_insert,
    append_data=True
)

# Task for the artists table
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    dim_table="artists",
    insert_sql=sql_statements.artist_table_insert,
    append_data=False
)

# Task for the time table
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    dim_table="time",
    insert_sql=sql_statements.time_table_insert,
    append_data=False
)


# Task for the quality tests
# The quality operator accepts a list of dict objects with both the SQL queries to check and their expected results
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    dq_checks=[
        {'test_sql': "SELECT COUNT(*) FROM users WHERE user_id is null", 'expected_result': 0},
        {'test_sql': "SELECT COUNT(*) FROM songs WHERE song_id is null", 'expected_result': 0},
        {'test_sql': "SELECT COUNT(*) FROM artists WHERE artist_id is null", 'expected_result': 0},
        {'test_sql': "SELECT COUNT(*) FROM time WHERE start_time is null", 'expected_result': 0}
    ]
)


# DAG ends with a end_execution task.
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)



# Task ordering
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator