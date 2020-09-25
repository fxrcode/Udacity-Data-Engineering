
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import datetime

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    csv_copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        TIMEFORMAT as 'epochmillisecs'
        JSON '{}'
        IGNOREHEADER 1
        DELIMITER ','
    """
    json_copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        TIMEFORMAT as 'epochmillisecs'
        JSON '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_path="",
                 file_format="JSON",
                 backfill=False,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.json_path = json_path
        self.file_format = file_format
        self.execution_date = kwargs.get('execution_date')
        self.backfill = backfill

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("TRUNCATE TABLE {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")

        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"
        if self.backfill:
            ed = datetime.datetime.strptime( self.execution_date ,'%Y-%m-%d')
            s3_path = f"{s3_path}/{str(ed.year)}/{str(ed.month)}"

        if self.file_format == "JSON":
            self.log.info(f"{self.s3_key} is JSON file")
            formatted_sql = StageToRedshiftOperator.json_copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.json_path,
            )
            redshift.run(formatted_sql)
        else:
            self.log.info(f"{self.s3_key} is CSV file")
            formatted_sql = StageToRedshiftOperator.csv_copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.json_path
            )
            redshift.run(formatted_sql)

        self.log.info("Done: copied {self.table} from S3 to Redshift")




