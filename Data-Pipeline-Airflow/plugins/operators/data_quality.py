from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """Custome operator to check data quality, to see if any table in the list is empty!

    Args:
        BaseOperator (BaseOperator): Abstract base class for all operators.
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 tables = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        """used postgres hook to run count on table, to check if table is empty or size = 0

        Args:
            context (context): Context is the same dictionary used as when rendering jinja templates.

        Raises:
            ValueError: raise if no records
            ValueError: raise if contains 0 row
        """
        self.log.info('DataQualityOperator start')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table in self.tables:
            records = redshift.get_records("SELECT COUNT(*) FROM {}".format(table))
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
        self.log.info('DataQualityOperator Done')