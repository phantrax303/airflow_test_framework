from typing import Any, Dict, List, Optional

from airflow.models.baseoperator import BaseOperator
from concurrent.futures import ThreadPoolExecutor, as_completed
from airflow.exceptions import AirflowException
import clickhouse_connect
import re
from airflow.hooks.base import BaseHook
import operator
import json



class TestOperator(BaseOperator):
    _PROHIBITED_SQL_WORDS = re.compile(
        r"\b(truncate|drop|replace|delete|alter|create|update|insert|grant|revoke)\b",
        re.IGNORECASE,
    )
    _BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)
    _LINE_COMMENT_RE  = re.compile(r"--.*?(?=\n|$)")
    _IDENTIFIER_RE    = re.compile(r"^[A-Za-z0-9_\.]+$")

    def __init__(
        self,
        conn_id: str,
        test_list: List[Dict[str, Any]],
        dst_database: str = 'default',
        dst_result_table: str = 'test_results_tbl',
        num_workers: int = 1,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)

        self.conn_id = conn_id
        # canonical list of table definitions to process
        self.test_list = test_list
        try:
            self.num_workers = int(num_workers)
        except ValueError:
            raise AirflowException(f"Invalid num_workers value: {num_workers}. Must be an integer.")
        self.dst_database = dst_database
        self.dst_result_table = dst_result_table
        self.op_kwargs = kwargs

    def execute(self, context: Dict[str, Any]) -> List[Dict[str, Any]]:

        results = []
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            futures: Dict[Any, Dict[str, Any]] = {}
            for test in self.test_list:
                fut = executor.submit(self.execute_test, test)
                futures[fut] = test
                self.log.debug(f"Submitted Test job for test_case: {test}")

            for future in as_completed(futures):
                test = futures[future]
                try:
                    result = future.result()
                    self.log.debug(f"Test finished for {test}: {result}")
                except Exception as e:
                    # preserve stack and optionally propagate depending on operator config
                    self.log.exception(f"Error while executing Test {test}: {e}")
                    result = self._build_result(test, passed=False)
                    result['error'] = str(e)
                results.append(result)

        self.log.info("All Test jobs completed")
        return results


    def execute_test(self, test: Dict[str, Any], **kwargs: Any) -> Dict[str, Any]:
        
        clickhouse_client: Optional[Any] = None
        passed = False

        self.validate_identifier(test.get('source_database'), test.get('source_table'), self.dst_database, self.dst_result_table)
        try:
            try:
                conn_data = BaseHook.get_connection(self.conn_id)
                clickhouse_client = clickhouse_connect.get_client(
                    host=conn_data.host,
                    port=conn_data.port,
                    database=conn_data.schema,  
                    user=conn_data.login,
                    password=conn_data.password
                )
            except Exception as e:
                self.log.exception(f"Error while trying to connect to database: {e}")
                return self._build_result(test, passed)
            
            # pre teste query execution
            pre_test_query = test.get('pre_test_query', '')
            try:
                params = json.loads(test.get('pre_test_params', '{}'))
            except json.JSONDecodeError as e:
                params = {}
                self.log.warning(f"Could not decode pre_test_params for test {test.get('test_name')}: {e}. Proceeding with empty params.")

            pre_test_query = pre_test_query.format(
                                    test_name= test.get('test_name'),
                                    source_database=test.get('source_database'),
                                    source_table=test.get('source_table'),
                                    interval_minutes = int(test.get('interval_minutes')),
                                    **params)
            self.validate_query(pre_test_query)
            query = f"""INSERT INTO 
                        {self.dst_database}.{self.dst_result_table} 
                    select 
                        toStartOfInterval(addMinutes(now(), 1), Interval {int(test.get('interval_minutes'))} minute) test_time_window,
                        {int(test.get('interval_minutes'))} `interval`,
                        '{test.get('test_name')}' test_case, 
                        ({pre_test_query})::JSON results"""
            try:
                clickhouse_client.command(query) 
            except Exception as e:
                self.log.exception(f"Error while trying to execute pre-test query to database: {e}")
                return self._build_result(test, passed)

            #Expectations validation
            expectations = test.get('expectations', [])
            concatenator = test.get('expectation_concatenator', 'AND')
            if concatenator not in ['AND', 'OR']:
                raise AirflowException(f"Invalid expectation_concatenator: {concatenator}. Must be 'AND' or 'OR'.")
            query = f'''select
                            *
                        from
                            {self.dst_database}.{self.dst_result_table}
                        where
                            test_case = '{test.get('test_name')}'
                            and test_time_window = toStartOfInterval(addMinutes(now(), 1), Interval {int(test.get('interval_minutes'))} minute) 
                            and not ({concatenator.join([f"(results.{exp.get('column')} {exp.get('operator')} results.{exp.get('value')})" for exp in expectations])})'''
            self.validate_query(query)
            test_result = None
            try:
                test_result = clickhouse_client.query(query).result_rows
            except Exception as e:
                self.log.exception(f"Error while trying to execute test query to database: {e}")
                return self._build_result(test, passed)

            if test_result is not None:
                passed = len(test_result)<=0
                 
            
            return self._build_result(test, passed)

        finally:
            if clickhouse_client:
                clickhouse_client.close()

    def validate_identifier(self, *identifiers: str) -> None:
        for val in identifiers:
            if not val or not self._IDENTIFIER_RE.match(val):
                raise AirflowException(
                    f"Invalid identifier '{val}'. Only alphanumeric characters, underscores and dots are allowed."
                )

    def validate_query(self, *queries: str) -> None:
        for query in queries:
            if not query or not query.strip():
                raise AirflowException("Query must not be empty.")

            if query.count('/*') != query.count('*/'):
                raise AirflowException("Unclosed block comment in query.")

            stripped = self._BLOCK_COMMENT_RE.sub('', query)
            stripped = self._LINE_COMMENT_RE.sub('', stripped)

            if ';' in stripped:
                raise AirflowException("Semicolons are not allowed in queries.")

            if not re.match(r'^\s*select\b', stripped, re.IGNORECASE):
                raise AirflowException("Only SELECT queries are allowed.")

            if self._PROHIBITED_SQL_WORDS.search(stripped):
                raise AirflowException("Prohibited SQL keyword detected in query.")

    def _build_result(self, test: Dict[str, Any], passed: bool = False) -> Dict[str, Any]:
        return {
            'test_name': test.get('test_name'),
            'status': passed,
            'test_interval': int(test.get('interval_minutes')),
            'criticality': test.get('criticality'),
            'source_database': test.get('source_database'),
            'source_table': test.get('source_table'),
            'test_type_key': test.get('test_type_key'),
        }