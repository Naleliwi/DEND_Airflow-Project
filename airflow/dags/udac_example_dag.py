from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'sparkify',
    'start_date': datetime(2019, 1, 12),
    'email': 'noofaleliwi@gmail.com',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3, # on failure the task will retry three times
    'retry_delay': timedelta(minutes= 5), # retry happens every five minutes
    'depends_on_past': False, # the dag will not have dependincies on past runs
    'catchup': False # No backfilling
}

dag = DAG('sparkify',
          default_args=default_args, #calling the dictionary above
          description='An automated pipeline that load data to Redshift in hourly bases',
          schedule_interval='0 * * * *', #hourly in cron time syntax
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table= '[public].staging_events',
    redshift_conn_id= 'redshift',
    aws_credentials_id = 'aws_default',
    s3_bucket = 'udacity-dend',
    s3_key = 'log_data',
    copy_options="JSON 's3://udacity-dend/log_json_path.json'"

)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table='[public].staging_songs',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_default',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    copy_options="FORMAT AS JSON 'auto'"
    
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table='[public].songplays',
    redshift_conn_id= 'redshift',
    sql = SqlQueries.songplay_table_insert # classname.methodname
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table='[public].users',
    redshift_conn_id= 'redshift',
    sql = SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table='[public].songs',
    redshift_conn_id= 'redshift',
    sql = SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table='[public].artists',
    redshift_conn_id= 'redshift',
    sql = SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='[public].time',
    redshift_conn_id= 'redshift',
    sql = SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    provide_context=True,
    redshift_conn_id= 'redshift',
    aws_credentials_id='aws_default',
    tables = ['artists', 'songplays', 'songs', 'time', 'users']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# Tasks Order

start_operator >> [stage_events_to_redshift,
                   stage_songs_to_redshift] >> load_songplays_table >> [load_user_dimension_table,
                                                                        load_song_dimension_table,
                                                                        load_artist_dimension_table,
                                                                        load_time_dimension_table] >> run_quality_checks >> end_operator
