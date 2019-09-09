from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    reload_sql = """
        TRUNCATE TABLE {};
        INSERT INTO {}
        {};
        COMMIT;
    """
    append_sql = """
               INSERT INTO {}
               {};
               COMMIT;
             """


    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 reload="",
                 table="",
                 sql_stmt="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.reload = reload
        self.table = table
        self.sql_stmt = sql_stmt

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        print "Loading dimension table"
        if (self.reload):
            formatted_sql = LoadDimensionOperator.reload_sql.format(
            self.table,
            self.table,
            self.sql_stmt
        )
        else:
            formatted_sql = LoadDimensionOperator.append_sql.format(
            self.table,
            self.sql_stmt
        )
        redshift.run(formatted_sql)
