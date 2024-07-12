from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    insert_sql = """
        INSERT INTO {}({});
    """
    @apply_defaults
    def __init__(self, redshift_conn_id="", table_name="", sql="", *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name  
        self.sql = sql

    def execute(self):
        """execute insert data from fact into dimension
        """        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Truncate {} dimension table".format(self.table_name))

        redshift.run("TRUNCATE {}".format(self.table_name)) 
        self.log.info("Insert data from staging into {} dimension table".format(self.table_name))

        formatted_sql = self.insert_sql.format(self.table_name,self.sql)
        redshift.run(formatted_sql)
