from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    select_sql = """
                 select count(*)
                 from {};

                 """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 tables,
                 redshift_conn_id,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        self.log.info('DataQualityOperator implemented started')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for row in self.tables:
            formatted_sql = DataQualityOperator.select_sql.format(row)
            records = redshift.get_records(formatted_sql)
            if len(records) < 1 :
                print "Data Quality check failed"
                #self.log.info(f"Data quality check failed. {row} returned no results")
            else:
                print "Data Quality check passed"
                #self.log.info(f"Data quality check passed. {row} returned {records} results")
