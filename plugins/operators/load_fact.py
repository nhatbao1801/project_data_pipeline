from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    insert_sql = """
        INSERT INTO {}({});
    """

    @apply_defaults
    def __init__(self, redshift_conn_id="", table_name="", sql="", append_optional=False, *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name  
        self.sql = sql
        self.append_optional = append_optional

    def execute(self):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if not self.append_optional:
            self.log.info("Truncate {} fact table".format(self.table_name))
            redshift.run("TRUNCATE {}".format(self.table_name))

        self.log.info("Insert data from staging into {} fact table".format(self.table_name))

        formatted_sql = self.insert_sql.format(self.table_name,self.sql)
        redshift.run(formatted_sql)
