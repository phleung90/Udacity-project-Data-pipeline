from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False, 
    'retries': 3, 
    'retry_delay': timedelta(minutes=5), 
    'catchup': False, 
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id='redshift', 
    aws_credentials_id='aws_credentials',
    table_name='staging_events', 
    s3_bucket='udacity_dend', 
    s3_key='log_data', 
    region='us-west-2',
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    redshift_conn_id='redshift', 
    aws_credentials_id='aws_credentials',
    table_name='staging_events', 
    s3_bucket='udacity_dend', 
    s3_key='song_data', 
    region='us-west-2',   
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id='redshift',
    table_name = 'songplays',
    songplay_sql = SqlQueries.songplay_table_insert, 
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id = 'redshift', 
    table_name = 'user', 
    dimension_sql = SqlQueries.user_table_insert, 
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id = 'redshift', 
    table_name = 'song',
    dimension_sql = SqlQueries.song_table_insert, 
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id = 'redshift', 
    table_name = 'artist',
    dimension_sql = SqlQueries.artist_table_insert, 
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id = 'redshift',
    table_name = 'time',   
    dimension_sql = SqlQueries.time_table_insert,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id = 'redshift',
    table_name = ["songplay", "users", "song", "artist", "time"],
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> stage_events_to_redshift 
start_operator >> stage_songs_to_redshift
( stage_events_to_redshift, stage_songs_to_redshift) >> load_songplays_table
load_songplays_table >> load_user_dimension_table 
load_songplays_table >> load_song_dimension_table 
load_songplays_table >> load_artist_dimension_table 
load_songplays_table >> load_time_dimension_table 
(load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table) >> run_quality_checks
run_quality_checks >> end_operator

