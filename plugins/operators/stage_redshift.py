from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    copy_sql = """
        COPY {} FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        {};
    """

    @apply_defaults
    def __init__(self,redshift_conn_id="", aws_conn_id="", table_name="",s3_path="", region="",data_format="",*args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.aws_conn_id=aws_conn_id
        self.table_name=table_name
        self.s3_path=s3_path
        self.region=region
        self.data_format=data_format

    def execute(self, context):
        """## _summary_

        ### Args:
            - `context (_type_)`: _description_
        """        
        aws_hook = AwsHook("aws_credentials")
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook("redshift")

        self.log.info("Truncate data from destination Redshift table")
        redshift_hook.run("TRUNCATE {}".format(self.table_name))

        self.log.info("Copying data from S3 to Redshift")
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table_name, 
                self.s3_path, 
                credentials.access_key,
                credentials.secret_key, 
                self.region,
                self.data_format
            )
        redshift_hook.run(formatted_sql)
        self.log.info('Copy data complete !!!')





