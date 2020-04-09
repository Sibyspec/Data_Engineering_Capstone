from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators import (StageToRedshiftOperator,LoadFactOperator,LoadDimensionOperator,DataQualityOperator)
from helpers import SqlQueries

"""
This python file makes call to all the ETL process involved. This is the mother file.
The DAG and schedule is defined in this file.
"""

default_args = {
    'owner': 'Siby',
    'start_date': datetime(2020, 4, 8),
    'Catchup': False,
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False    
}

dag = DAG('airbnb_dag',
          default_args=default_args,
          description='Extract,Load and transform data in Redshift with Airflow',
          schedule_interval='0 0 * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


create_fact_dim_tables_task = PostgresOperator(
    task_id='create_tables',
    dag=dag,
    sql='create_airbnb_tables.sql',
    postgres_conn_id="redshift"
)

stage_listings_to_redshift = StageToRedshiftOperator(
    task_id='Stage_listing',
    dag=dag,
    table="staging_listings",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="sibycapstone",
    s3_key='capstone/'
)

stage_reviews_to_redshift = StageToRedshiftOperator(
    task_id='Stage_reviews',
    dag=dag,
    table="staging_reviews",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="sibycapstone",
    s3_key="review/"
)


load_listings_table = LoadFactOperator(
    task_id='Load_listings_fact_table',
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.listings_table_insert,
    database='siby'
)

load_hosts_dimension_table = LoadDimensionOperator(
    task_id='Load_hosts_dim_table',
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.host_table_insert,
    database="siby",
    truncate_before_insert=1,
    table="hosts"
)

load_reviews_dimension_table = LoadDimensionOperator(
    task_id='Load_reviews_dim_table',
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.review_table_insert,
    database="siby",
    truncate_before_insert=1,
    table="listing_reviews"
)

load_property_dimension_table = LoadDimensionOperator(
    task_id='Load_property_dim_table',
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.property_table_insert,
    database="siby",
    truncate_before_insert=1,
    table="listing_property_type"
)

load_room_dimension_table = LoadDimensionOperator(
    task_id='Load_room_dim_table',
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.room_table_insert,
    database="siby",
    truncate_before_insert=1,
    table="listing_room_type"
)

load_address_dimension_table = LoadDimensionOperator(
    task_id='Load_address_dim_table',
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.address_table_insert,
    database="siby",
    truncate_before_insert=1,
    table="listing_address"
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_fact_dim_tables_task
create_fact_dim_tables_task >> stage_listings_to_redshift
create_fact_dim_tables_task >> stage_reviews_to_redshift
stage_listings_to_redshift >> load_listings_table
stage_reviews_to_redshift >> load_listings_table
load_listings_table >> load_hosts_dimension_table
load_listings_table >> load_reviews_dimension_table
load_listings_table >> load_property_dimension_table
load_listings_table >> load_room_dimension_table
load_listings_table >> load_address_dimension_table
load_listings_table >> run_quality_checks
load_hosts_dimension_table >> run_quality_checks
load_reviews_dimension_table >> run_quality_checks
load_property_dimension_table >> run_quality_checks
load_room_dimension_table >> run_quality_checks
load_address_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
