from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id, 
                 table_name = [],            
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name


    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        
        for table in self.table_name: 
            table_exists = redshift.get_first("SELECT * FROM SELECT * FROM information_schema.tables WHERE table_name = '{}';".format(table))                
            records = redshift.get_records("SELECT COUNT(*) FROM {}".format(table))  
            
            if table_exists:
                num_records = records[0][0]
                if num_records == 0:
                    self.log.error("No records in destination table {}".format(table))
                    raise ValueError("No records in destination {}".format(table))
                self.log.info("table {} is existed and with {} records".format(table, num_records))            
            else:
                raise ValueError("table {} does not exist".format(table))
            
