from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id, 
                 table_name, 
                 dimension_sql,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.dimension_sql = dimension_sql 

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        
        redshift_hook = PostgreHook(postgre_conn_id=self.redshift_conn_id)
        
        drop_table = f'DROP TABLE IF EXISTS {self.table_name}'
        create_table = f'CREATE TABLE IF NOT EXISTS {self.table_name}'
        
        table_insert_sql = f'''
            INSERT INTO {self.table_name}
            {self.dimension_sql}
        '''
    
        redshift_hook.run(drop_table)
        redshift_hook.run(create_table)
        redshift_hook.run(table_insert_sql)
