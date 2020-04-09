from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    OPerator to load dimension tables from fact table.    
    
    :sql sql (The SQL statements are taken from Helper class)
    :redshift_conn_id str (the connection details to connect to redshift database)
    :database str (the redshift database name)
    :truncate before insert int ( 1 or 0 )
    :table str (the staging table name that should be loaded)
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self, 
                 sql="",
                 postgres_conn_id="",
                 database="",
                 truncate_before_insert="",
                 table="",
                 autocommit=True,
                 parameters=None,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        self.database = database
        self.truncate_before_insert = truncate_before_insert
        self.table=table
        self.autocommit = autocommit
        self.parameters = parameters
        

    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id,schema=self.database)
        if self.truncate_before_insert==1:
            truncate_table = 'truncate table {}'.format(self.table)
            self.hook.run(truncate_table, self.autocommit, parameters=self.parameters)
            
        self.hook.run(self.sql, self.autocommit, parameters=self.parameters)
        for output in self.hook.conn.notices:
            self.log.info(output)