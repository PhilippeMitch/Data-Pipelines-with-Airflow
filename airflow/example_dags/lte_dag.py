from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.example_dags.plugins.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.example_dags.plugins.helpers import SqlQueries

from airflow.operators.postgres_operator import PostgresOperator

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'mitch',
    'depends_on_past': False,
    'start_date': datetime.now(),
     'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('lte_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          start_date=datetime.now()
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_task = PostgresOperator(
    task_id='create_tables',
    dag=dag,
    postgres_conn_id="redshift",
    sql='sql/create_tables.sql'
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id = 'redshift',
    aws_key = AWS_KEY,
    aws_secret = AWS_SECRET,
    table_name = 'staging_events',
    s3_bucket = 'udacity_dend',
    s3_key = 'log_data',
    copy_json_option="s3://udacity-dend/log_json_path.json",
    region = 'us-west-2'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_songs',
    dag = dag,
   redshift_conn_id = 'redshift',
    aws_key = AWS_KEY,
    aws_secret = AWS_SECRET,
    table_name = 'staging_songs',
    s3_bucket = 'udacity_dend',
    s3_key = 'log_data',
    copy_json_option="s3://udacity-dend/log_json_path.json",
    region = 'us-west-2'
)

load_songplays_table = LoadFactOperator(
    task_id = 'Load_songplays_fact_table',
    redshift_conn_id = "redshift",
    table_name = "songplays",
    query = SqlQueries.songplay_table_insert,
)
 
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table_name = 'users',
    truncate_table = False,
    query = SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table_name = 'songs',
    truncate_table = False,
    query = SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table_name = 'artists',
    truncate_table = False,
    query = SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table_name = 'time',
    truncate_table = False,
    query = SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = 'redshift',
    data_quality_checks = [
        {'query_stmt': 'select count(*) from public.songs where title is null', 'expected_value': 0},
        {'query_stmt': 'select count(*) from public.artists where name is null', 'expected_value': 0 },
        {'query_stmt': 'select count(*) from public.users where first_name is null', 'expected_value': 0},
        {'query_stmt': 'select count(*) from public.time where month is null', 'expected_value': 0},
        {'query_stmt': 'select count(*) from public.songsplay where userid is null', 'expected_value': 0 }
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# DAG Task Dependency
start_operator >> \
    create_tables_task >> [stage_events_to_redshift,
                           stage_songs_to_redshift] >> \
    load_songplays_table >> [load_song_dimension_table,
                             load_user_dimension_table,
                             load_artist_dimension_table,
                             load_time_dimension_table] >> \
    run_quality_checks >> \
    end_operator