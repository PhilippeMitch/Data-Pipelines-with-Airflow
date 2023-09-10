from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    """
    DAG operator used to populate fact tables.
    redshift_conn_id: string
                    reference to a specific redshift database
    table_name: string
                redshift table to load
    insert_sql: string
                statement used to extract songplays data
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table_name,
                 query,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.query = query

    def execute(self, context):
        self.log.info('LoadFactOperator Connection')
        # connect to Redshift
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Connected with {self.redshift_conn_id}")

        # build insert statement
        self.log.info(f"Inserting Data - to {self.table} ")
        insert_query_stmt = f"INSERT INTO {self.table_name} {self.query}"

        redshift_hook.run(insert_query_stmt)
        self.log.info(
            f"LoadDimensionOperator insert complete - {self.table_name}")