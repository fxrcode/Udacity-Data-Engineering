from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """Custom operator to load fact table from staging tables

    Args:
        BaseOperator (BaseOperator): Abstract base class for all operators.
    """

    ui_color = '#F98866'
    insert_sql ='''
                INSERT INTO {}
                {};
                '''

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 sql_query = "",
                 append_only = False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.append_only = append_only

    def execute(self, context):
        """override execute to do trunct-insert parttern on table

        Args:
            context (context): Context is the same dictionary used as when rendering jinja templates.
        """
        self.log.info('LoadFactOperator start')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if not self.append_only:
            self.log.info(f"Truncate fact table: {self.table}")
            redshift.run("TRUNCATE TABLE {}".format(self.table))
        self.log.info(f"Arm {self.table} fact table")
        formatted_sql = LoadFactOperator.insert_sql.format(
            self.table,
            self.sql_query
        )
        redshift.run(formatted_sql)
        self.log.info('LoadFactOperator Done')
