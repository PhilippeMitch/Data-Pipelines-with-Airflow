from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    DAG operator used for data quality checks.

    Params:
    -------
    redshift_conn_id: string
                        reference to a specific redshift database
    data_quality_checks: list
            data quality SQL stmts

    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 data_quality_checks = [],
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.data_quality_checks = data_quality_checks

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        self.log.info(f"Connected with {self.redshift_conn_id}")

        failed_tests = []

        for check_step in self.data_quality_checks:
            
            check_query_stmt = check_step.get('query_stmt')
            expected_result  = check_step.get('query_stmt')
            
            result = redshift_hook.get_records(check_query_stmt)[0]
            
            self.log.info(f"Running query   : {check_query_stmt}")
            self.log.info(f"Expected result : {expected_result}")
            self.log.info(f"Check result    : {result}")
            
            description = check_step.get('descr')
            exp_result = check_step.get('expected_result')
            
            if result[0] != expected_result:
                failed_tests.append(
                        f"{description}, expected {exp_result} got {result[0]}\n  "
                        "{sql}")
                self.log.info(f"Data quality check fails At   : {check_query_stmt}")

        if len(failed_tests) > 0:
            self.log.info('Data quality checks - Failed !')
        else:
            self.log.info('Data quality checks - Passed !')