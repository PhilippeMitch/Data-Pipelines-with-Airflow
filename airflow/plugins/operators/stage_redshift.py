# from airflow.hooks.postgres_hook import PostgresHook
# from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task
# from airflow.models import BaseOperator
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):

    """
    DAG operator to populate staging tables from source files.

    Params:
    -------
    redshift_conn_id: string
                    reference to a specific redshift database
    table_name: string
                redshift staging table to load
    s3_bucket: string
        S3 bucket location
    aws_key: string
            AWS user key
    aws_secret: string
                AWS user secret
    region: string
            S3 bucket location

    """
    
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 aws_key, aws_secret,
                 table_name,s3_bucket, s3_key,
                 region, copy_json_option,
                 *args, **kwargs) -> None:

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_key = aws_key
        self.aws_secret = aws_secret
        self.table = table_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.copy_json_option = copy_json_option
        self.region = region
    
    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        # connect to Redshift
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Connected with {self.redshift_conn_id}")

        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        sql_stmt = f"""
            COPY {self.table_name} 
                FROM 's3://{self.s3_bucket}/{self.s3_path}' 
                ACCESS_KEY_ID '{self.aws_key}'
                SECRET_ACCESS_KEY '{self.aws_secret}'
                REGION '{self.region}'
                JSON '{self.copy_json_option}'
                TIMEFORMAT as 'epochmillisecs'
                TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
        """
        self.log.info(f" Copying data from '{s3_path}' to '{self.table}'")

        redshift_hook.run(sql_stmt)
        self.log.info(
            f"StageToRedshiftOperator copy complete - {self.table_name}")
        