from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    """
    DAG operator used to populate dimension tables  in a STAR schema.

    Param:
    ------
    redshift_conn_id: string
                    reference to a specific redshift database
    table_name: string
                redshift dimension table to load
    query: string
            statement used to extract songplays data

    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table_name,
                 query,
                 truncate_table,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.query = query
        self.truncate_table = truncate_table

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        # connect to Redshift
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Connected with {self.redshift_conn_id}")

        # Build truncate statement if required
        if self.truncate_table == True:
            self.log.info(f"Truncating table {self.table_name} before adding records")
            redshift_hook.run(f"TRUNCATE TABLE {self.table_name}")
            self.log.info(f"Truncating table {self.table_name} complete!")

        # build insert statement
        self.log.info(f"Inserting Data - to {self.table} ")
        insert_query_stmt = f"INSERT INTO {self.table_name} {self.query}"

        redshift_hook.run(insert_query_stmt)
        self.log.info(
            f"LoadDimensionOperator insert complete - {self.table_name}")