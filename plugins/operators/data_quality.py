import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Executes sql code in a specific Redshift database.
    Operator to perform data checks in all dimension and fact tables.
    It checks where the dimension and fact tables are actually loaded with records.
    
    :redshift_comm_id str
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                *args, **kwargs):

         super(DataQualityOperator, self).__init__(*args, **kwargs)
         self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
         redshift_hook = PostgresHook(self.redshift_conn_id)
         fact_records = redshift_hook.get_records("SELECT COUNT(*) FROM listings")
         num_records_fact = fact_records[0][0]
         if num_records_fact < 1:
            raise ValueError("Data quality check failed. Listings contained 0 rows")
         logging.info(f"Data quality on fact table Listings. check passed with {fact_records[0][0]} records")
        
         hosts_dim_records = redshift_hook.get_records("SELECT COUNT(*) FROM hosts")
         num_records_dim_hosts = hosts_dim_records[0][0]
         if num_records_dim_hosts < 1:
            raise ValueError("Data quality check failed. Hosts contained 0 rows")
         logging.info(f"Data quality on dimension table Hosts. check passed with {hosts_dim_records[0][0]} records")
        
         review_dim_records = redshift_hook.get_records("SELECT COUNT(*) FROM listing_reviews")
         num_records_dim_review = review_dim_records[0][0]
         if num_records_dim_review < 1:
            raise ValueError("Data quality check failed. LISTING_REVIEWS contained 0 rows")
         logging.info(f"Data quality on dimension table LISTING_REVIEWS. check passed with {review_dim_records[0][0]} records")

         property_dim_records = redshift_hook.get_records("SELECT COUNT(*) FROM listing_property_type")
         num_records_dim_property = property_dim_records[0][0]
         if num_records_dim_property < 1:
            raise ValueError("Data quality check failed. LISTING_PROPERTY_TYPE contained 0 rows")
         logging.info(f"Data quality on dimension table LISTING_PROPERTY_TYPE. check passed with {property_dim_records[0][0]} records")

         room_dim_records = redshift_hook.get_records("SELECT COUNT(*) FROM listing_room_type")
         num_records_dim_room = room_dim_records[0][0]
         if num_records_dim_room < 1:
            raise ValueError("Data quality check failed. LISTING_ROOM_TYPE contained 0 rows")
         logging.info(f"Data quality on dimension table LISTING_ROOM_TYPE. check passed with {room_dim_records[0][0]} records")
        
         address_dim_records = redshift_hook.get_records("SELECT COUNT(*) FROM listing_address")
         num_records_dim_addres = address_dim_records[0][0]
         if num_records_dim_addres < 1:
            raise ValueError("Data quality check failed. LISTING_ADDRESS contained 0 rows")
         logging.info(f"Data quality on dimension table LISTING_ADDRESS. check passed with {address_dim_records[0][0]} records")