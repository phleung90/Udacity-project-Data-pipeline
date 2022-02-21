from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    clear_existing_data = "DELETE FROM {}"
    
    copy_data_to_redshift = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION AS '{}'
    """
   
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id, 
                 aws_credentials_id, 
                 table_name, 
                 s3_bucket, 
                 s3_key, 
                 region,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        
        
    def execute(self, context):
        # Connect to S3
        self.log.info('Connect to S3')
        aws_hook = AwsHook(self.aws_credentials_id)
        aws_credentials = aws_hook.get_credentials()
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)

        
        # Connect to redshift 
        self.log.info('Connect to aws')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        
        # Check if there is any record in the existing table. If yes, truncate the table. If no, then continue 
        records = redshift_hook.get_records(f'''SELECT COUNT(*) FROM {self.table_name}''')
        if records > 0:
          redshift_hook.run(clear_existing_data.format(self.table_name))
        
        # Copy the data from S3 to redshift
        copy_to_redshift = StageToRedshiftOperator.copy_data_to_redshift.format(
            self.table_name,
            s3_path,
            aws_credentials.access_key,
            aws_credentials.secret_key,
            self.region
        )
        
        redshift_hook.run(copy_to_redshift)
        