from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Operator to load fact table from staging tables.    
    
    :sql sql (The SQL statements are taken from Helper class)
    :redshift_conn_id str (the connection details to connect to redshift database)
    :database str (the redshift database name)
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self, 
                 sql,
                 postgres_conn_id,
                 database,
                 autocommit=True,
                 parameters=None,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        self.database = database
        self.autocommit = autocommit
        self.parameters = parameters
        

    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id,schema=self.database)
        self.hook.run(self.sql, self.autocommit, parameters=self.parameters)
        for output in self.hook.conn.notices:
            self.log.info(output)