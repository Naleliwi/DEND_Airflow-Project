from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'    
    
    @apply_defaults
    def __init__(self,
               
                 redshift_conn_id="redshift",
                 aws_credentials_id="aws_default",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 copy_options='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
    
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.copy_options = copy_options
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        
        # getting aws cedintials to connect with RDS
        aws_hook = AwsHook("aws_default")
        credentials = aws_hook.get_credentials() 
        redshift = PostgresHook("redshift")

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        
        
        # dynamically generating the copy statement using params
        
        copy_query = """
                    COPY {table}
                    FROM 's3://{s3_bucket}/{s3_prefix}'
                    with credentials
                    'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
                    {copy_options};
                """.format(table=self.table,
                           s3_bucket=self.s3_bucket,
                           s3_key=self.s3_key,
                           access_key=credentials.access_key,
                           secret_key=credentials.secret_key,
                           copy_options=self.copy_options)
        redshift.run(copy_query)





