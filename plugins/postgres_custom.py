import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)


class PostgreSQLCountRows(BaseOperator):

    @apply_defaults
    def __init__(self, schema, table, *args, **kwargs):
        self.schema = schema
        self.table = table
        self.hook = PostgresHook()
        super(PostgreSQLCountRows, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info(f"Executing count in {self.schema}.{self.table}")
        result = self.hook.get_first(f"SELECT COUNT(*) FROM {self.schema}.{self.table}")[0]
        log.info(f"Result is {result}")

        return result


class PostgreSQLCustomOperatorPlugin(AirflowPlugin):
    name = "postgres_custom"
    operators = [PostgreSQLCountRows]
