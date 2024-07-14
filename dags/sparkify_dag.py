from airflow.decorators import dag
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from airflow.operators.dummy_operator import DummyOperator
from helpers import sql_statement
from datetime import datetime, timedelta
from airflow.models import Variable


ARN = Variable.get('ARN')
LOG_DATA = Variable.get('LOG_DATA')
LOG_JSONPATH = Variable.get('LOG_JSONPATH')
SONG_DATA = Variable.get('SONG_DATA')

default_args = {
    'owner': 'baotcn',
    'start_date': datetime.now(),
    'retries':3,
    'email_on_failure':False,
    'retry_delay':timedelta(minutes=5),
    'catchup': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id="redshift",
        aws_conn_id="aws_credentials",
        table_name="staging_events",
        s3_path=LOG_DATA,
        region="us-west-2",
        data_format="FORMAT AS JSON '{}'".format(LOG_JSONPATH)
    )
    
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id="redshift",
        aws_conn_id="aws_credentials",
        table_name="staging_songs",
        s3_path=SONG_DATA,
        region="us-west-2",
        data_format="JSON 'auto' COMPUPDATE OFF"
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id="redshift",
        table_name="songplays",
        sql=sql_statement.SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        table_name="users",
        sql=sql_statement.SqlQueries.user_table_insert,
        append_optional=False
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",
        table_name="songs",
        sql=sql_statement.SqlQueries.song_table_insert,
        append_optional=False
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        table_name="artists",
        sql=sql_statement.SqlQueries.artist_table_insert,
        append_optional=False
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        table_name="time",
        sql=sql_statement.SqlQueries.time_table_insert,
        append_optional=False
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        tables=["songplays", "users", "songs", "artists", "time"]
    )

    end_operator = DummyOperator(task_id='Stop_execution')

    start_operator>>[stage_events_to_redshift,stage_songs_to_redshift]>>load_songplays_table
    load_songplays_table>>[load_user_dimension_table,load_song_dimension_table,load_artist_dimension_table,load_time_dimension_table]>>\
    run_quality_checks>>end_operator


final_project_dag = final_project()