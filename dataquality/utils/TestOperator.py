"""
TestOperator module for executing data quality tests against a ClickHouse database.

This module defines the :class:`TestOperator`, an Airflow custom operator that
takes a list of test definitions, executes each test concurrently using a
thread pool, validates pre-test queries, stores intermediate results back into
ClickHouse, and evaluates configurable expectations to determine pass/fail status.

Typical usage inside a DAG::

    TestOperator.partial(
        task_id='testing_task',
        conn_id='airflow_datapipe',
        num_workers=3,
        dst_database='internal_engineering',
        dst_result_table='result_tests_tbl',
    ).expand(test_list=tests_lists)
"""

from typing import Any, Dict, List, Optional

from airflow.models.baseoperator import BaseOperator
from concurrent.futures import ThreadPoolExecutor, as_completed
from airflow.exceptions import AirflowException
import clickhouse_connect
import re
from airflow.hooks.base import BaseHook
import json


class TestOperator(BaseOperator):
    """Airflow operator that executes a batch of data-quality tests against ClickHouse.

    Each test definition is a dictionary retrieved from the configuration table.
    The operator:

    1. Runs a *pre-test* query (typically a metrics/aggregation SELECT) and writes
       the result as a JSON row into the destination result table.
    2. Reads that row back and evaluates a set of *expectations* (column/operator/value
       triplets) combined with a logical concatenator (AND / OR).
    3. Returns a list of result dictionaries — one per test — indicating pass/fail
       status, which are pushed to XCom for downstream tasks (e.g. Slack alerting).

    Args:
        conn_id (str): Airflow connection ID pointing to the ClickHouse instance.
        test_list (List[Dict[str, Any]]): List of test definition dictionaries.
            Each dict is expected to contain at minimum:
            ``test_name``, ``source_database``, ``source_table``,
            ``pre_test_query``, ``pre_test_params``, ``expectations``,
            ``expectation_concatenator``, ``interval_minutes``, and ``criticality``.
        dst_database (str): ClickHouse database where test results are stored.
            Defaults to ``'default'``.
        dst_result_table (str): ClickHouse table where test results are stored.
            Defaults to ``'test_results_tbl'``.
        num_workers (int): Number of parallel threads used to execute tests.
            Defaults to ``1``.
        **kwargs: Additional keyword arguments forwarded to :class:`~airflow.models.BaseOperator`.

    Raises:
        AirflowException: If ``num_workers`` cannot be cast to ``int``.
    """

    # ------------------------------------------------------------------ #
    # Compiled regex constants shared across all instances                #
    # ------------------------------------------------------------------ #

    #: SQL keywords that are never allowed in user-supplied queries.
    _PROHIBITED_SQL_WORDS = re.compile(
        r"\b(truncate|drop|replace|delete|alter|create|update|insert|grant|revoke)\b",
        re.IGNORECASE,
    )
    #: Matches block comments ``/* … */`` (including multi-line).
    _BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)
    #: Matches single-line SQL comments ``-- …``.
    _LINE_COMMENT_RE = re.compile(r"--.*?(?=\n|$)")
    #: Valid SQL/ClickHouse identifier pattern (alphanumeric, underscore, dot).
    _IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_\.]+$")

    def __init__(
        self,
        conn_id: str,
        test_list: List[Dict[str, Any]],
        dst_database: str = "default",
        dst_result_table: str = "test_results_tbl",
        num_workers: int = 1,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)

        self.conn_id = conn_id
        self.test_list = test_list

        try:
            self.num_workers = int(num_workers)
        except (ValueError, TypeError) as exc:
            raise AirflowException(
                f"Invalid num_workers value: {num_workers}. Must be an integer."
            ) from exc

        self.dst_database = dst_database
        self.dst_result_table = dst_result_table
        self.op_kwargs = kwargs

    # ------------------------------------------------------------------ #
    # Operator entry-point                                                 #
    # ------------------------------------------------------------------ #

    def execute(self, context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Execute all tests in :attr:`test_list` concurrently and return results.

        This method is invoked by the Airflow scheduler when the task runs.
        It spins up a :class:`~concurrent.futures.ThreadPoolExecutor` with
        :attr:`num_workers` threads and submits one :meth:`execute_test` call per
        test definition.  Results (including failures) are collected and returned
        as a list, which is automatically pushed to XCom.

        Args:
            context (Dict[str, Any]): Airflow task-instance context dictionary.

        Returns:
            List[Dict[str, Any]]: One result dict per test.  Each dict contains at
            minimum ``test_name``, ``status`` (bool), ``test_interval``,
            ``criticality``, ``source_database``, ``source_table``, and
            ``test_type_key``.  Failed tests also carry an ``error`` key.
        """
        self.log.info(
            "Starting TestOperator execution | tests=%d | num_workers=%d | "
            "dst=%s.%s",
            len(self.test_list),
            self.num_workers,
            self.dst_database,
            self.dst_result_table,
        )

        results: List[Dict[str, Any]] = []

        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            futures: Dict[Any, Dict[str, Any]] = {}

            for test in self.test_list:
                test_name = test.get("test_name", "<unnamed>")
                self.log.info("Submitting test '%s' to thread pool.", test_name)
                fut = executor.submit(self.execute_test, test)
                futures[fut] = test

            self.log.info(
                "All %d tests submitted. Waiting for completion…", len(futures)
            )

            for future in as_completed(futures):
                test = futures[future]
                test_name = test.get("test_name", "<unnamed>")
                try:
                    result = future.result()
                    status_label = "PASSED" if result.get("status") else "FAILED"
                    self.log.info(
                        "Test '%s' completed with status: %s.", test_name, status_label
                    )
                except Exception as exc:
                    self.log.exception(
                        "Unhandled exception in test '%s': %s", test_name, exc
                    )
                    result = self._build_result(test, passed=False)
                    result["error"] = str(exc)

                results.append(result)

        passed_count = sum(1 for r in results if r.get("status"))
        self.log.info(
            "TestOperator finished | total=%d | passed=%d | failed=%d",
            len(results),
            passed_count,
            len(results) - passed_count,
        )
        return results

    # ------------------------------------------------------------------ #
    # Per-test execution logic                                             #
    # ------------------------------------------------------------------ #

    def execute_test(self, test: Dict[str, Any], **kwargs: Any) -> Dict[str, Any]:
        """Execute a single data-quality test and return its result.

        The method performs the following steps:

        1. **Identifier validation** — ensures source/destination database and table
           names contain only safe characters.
        2. **ClickHouse connection** — opens a dedicated client for this thread.
        3. **Pre-test query** — formats and validates the pre-test SELECT, then
           inserts the query result as a JSON row into the result table.
        4. **Expectation evaluation** — queries the just-inserted row and applies
           the configured expectations to determine pass/fail.
        5. **Cleanup** — closes the ClickHouse client in a ``finally`` block.

        Args:
            test (Dict[str, Any]): Single test definition dictionary.
            **kwargs: Unused; present for forward-compatibility.

        Returns:
            Dict[str, Any]: Result dictionary produced by :meth:`_build_result`.
            On failure during any step, returns a result with ``status=False`` and
            an ``error`` key describing the problem.
        """
        test_name = test.get("test_name", "<unnamed>")
        self.log.info("execute_test | START | test='%s'", test_name)

        clickhouse_client: Optional[Any] = None
        passed = False

        # ── 1. Identifier validation ────────────────────────────────────
        self.log.debug(
            "execute_test | Validating identifiers for test='%s' | "
            "source_database='%s' source_table='%s' dst_database='%s' dst_table='%s'",
            test_name,
            test.get("source_database"),
            test.get("source_table"),
            self.dst_database,
            self.dst_result_table,
        )
        self.validate_identifier(
            test.get("source_database"),
            test.get("source_table"),
            self.dst_database,
            self.dst_result_table,
        )
        self.log.debug(
            "execute_test | Identifiers valid for test='%s'.", test_name
        )

        try:
            # ── 2. Open ClickHouse connection ───────────────────────────
            self.log.debug(
                "execute_test | Opening ClickHouse connection | "
                "conn_id='%s' | test='%s'",
                self.conn_id,
                test_name,
            )
            try:
                conn_data = BaseHook.get_connection(self.conn_id)
                clickhouse_client = clickhouse_connect.get_client(
                    host=conn_data.host,
                    port=conn_data.port,
                    database=conn_data.schema,
                    user=conn_data.login,
                    password=conn_data.password,
                )
                self.log.info(
                    "execute_test | ClickHouse connection established | "
                    "host='%s' port=%s | test='%s'",
                    conn_data.host,
                    conn_data.port,
                    test_name,
                )
            except Exception as exc:
                self.log.exception(
                    "execute_test | Failed to connect to ClickHouse | "
                    "conn_id='%s' | test='%s' | error: %s",
                    self.conn_id,
                    test_name,
                    exc,
                )
                return self._build_result(test, passed)

            # ── 3. Pre-test query ───────────────────────────────────────
            pre_test_query_raw = test.get("pre_test_query", "")
            self.log.debug(
                "execute_test | Raw pre_test_query for test='%s': %s",
                test_name,
                pre_test_query_raw,
            )

            try:
                params = json.loads(test.get("pre_test_params", "{}"))
                self.log.debug(
                    "execute_test | Parsed pre_test_params for test='%s': %s",
                    test_name,
                    params,
                )
            except json.JSONDecodeError as exc:
                params = {}
                self.log.warning(
                    "execute_test | Could not decode pre_test_params for test='%s': %s. "
                    "Proceeding with empty params.",
                    test_name,
                    exc,
                )

            pre_test_query = pre_test_query_raw.format(
                test_name=test.get("test_name"),
                source_database=test.get("source_database"),
                source_table=test.get("source_table"),
                interval_minutes=int(test.get("interval_minutes")),
                **params,
            )
            self.log.debug(
                "execute_test | Formatted pre_test_query for test='%s': %s",
                test_name,
                pre_test_query,
            )

            self.validate_query(pre_test_query)
            self.log.debug(
                "execute_test | pre_test_query passed validation for test='%s'.",
                test_name,
            )

            insert_query = f"""INSERT INTO
                        {self.dst_database}.{self.dst_result_table}
                    select
                        toStartOfInterval(addMinutes(now(), 1), Interval {int(test.get('interval_minutes'))} minute) test_time_window,
                        {int(test.get('interval_minutes'))} `interval`,
                        '{test.get('test_name')}' test_case,
                        ({pre_test_query})::JSON results"""
            self.log.debug(
                "execute_test | INSERT query for test='%s': %s",
                test_name,
                insert_query,
            )

            try:
                clickhouse_client.command(insert_query)
                self.log.info(
                    "execute_test | Pre-test result successfully inserted | "
                    "dst='%s.%s' | test='%s'",
                    self.dst_database,
                    self.dst_result_table,
                    test_name,
                )
            except Exception as exc:
                self.log.exception(
                    "execute_test | Failed to INSERT pre-test result | "
                    "test='%s' | error: %s",
                    test_name,
                    exc,
                )
                return self._build_result(test, passed)

            # ── 4. Expectation evaluation ───────────────────────────────
            expectations = test.get("expectations", [])
            concatenator = test.get("expectation_concatenator", "AND")
            self.log.info(
                "execute_test | Evaluating %d expectation(s) with concatenator='%s' "
                "| test='%s'",
                len(expectations),
                concatenator,
                test_name,
            )

            if concatenator not in ("AND", "OR"):
                raise AirflowException(
                    f"Invalid expectation_concatenator: '{concatenator}'. "
                    "Must be 'AND' or 'OR'."
                )

            expectation_clauses = concatenator.join(
                [
                    f"(results.{exp.get('column')} {exp.get('operator')} results.{exp.get('value')})"
                    for exp in expectations
                ]
            )
            select_query = f"""select
                            *
                        from
                            {self.dst_database}.{self.dst_result_table}
                        where
                            test_case = '{test.get('test_name')}'
                            and test_time_window = toStartOfInterval(addMinutes(now(), 1), Interval {int(test.get('interval_minutes'))} minute)
                            and not ({expectation_clauses})"""
            self.log.debug(
                "execute_test | Expectation SELECT query for test='%s': %s",
                test_name,
                select_query,
            )

            self.validate_query(select_query)
            self.log.debug(
                "execute_test | Expectation query passed validation for test='%s'.",
                test_name,
            )

            test_result = None
            try:
                test_result = clickhouse_client.query(select_query).result_rows
                self.log.info(
                    "execute_test | Expectation query returned %d violation row(s) "
                    "| test='%s'",
                    len(test_result),
                    test_name,
                )
            except Exception as exc:
                self.log.exception(
                    "execute_test | Failed to run expectation query | "
                    "test='%s' | error: %s",
                    test_name,
                    exc,
                )
                return self._build_result(test, passed)

            if test_result is not None:
                passed = len(test_result) <= 0
                self.log.info(
                    "execute_test | Test='%s' result: %s (violation_rows=%d).",
                    test_name,
                    "PASSED" if passed else "FAILED",
                    len(test_result),
                )

            return self._build_result(test, passed)

        finally:
            if clickhouse_client:
                self.log.debug(
                    "execute_test | Closing ClickHouse connection for test='%s'.",
                    test_name,
                )
                clickhouse_client.close()
            self.log.info("execute_test | END | test='%s' | passed=%s", test_name, passed)

    # ------------------------------------------------------------------ #
    # Validation helpers                                                   #
    # ------------------------------------------------------------------ #

    def validate_identifier(self, *identifiers: str) -> None:
        """Validate that all provided identifiers are safe for use in SQL statements.

        An identifier is considered valid if it matches ``^[A-Za-z0-9_\\.]+$``,
        preventing SQL-injection through database/table names.

        Args:
            *identifiers (str): One or more identifier strings to validate
                (e.g., database names, table names).

        Raises:
            AirflowException: If any identifier is empty or contains characters
                outside the allowed set.
        """
        for val in identifiers:
            self.log.debug("validate_identifier | Checking identifier: '%s'", val)
            if not val or not self._IDENTIFIER_RE.match(val):
                self.log.error(
                    "validate_identifier | INVALID identifier detected: '%s'", val
                )
                raise AirflowException(
                    f"Invalid identifier '{val}'. "
                    "Only alphanumeric characters, underscores and dots are allowed."
                )
        self.log.debug(
            "validate_identifier | All %d identifier(s) are valid.", len(identifiers)
        )

    def validate_query(self, *queries: str) -> None:
        """Validate that each query is a safe SELECT statement.

        Performs the following checks in order:

        1. Query must not be empty or blank.
        2. Block comments ``/* … */`` must be balanced.
        3. After stripping comments, the query must not contain semicolons.
        4. The first meaningful token must be ``SELECT``.
        5. No prohibited DML/DDL keywords (``DROP``, ``INSERT``, etc.) may appear.

        Args:
            *queries (str): One or more SQL query strings to validate.

        Raises:
            AirflowException: On the first validation failure encountered, with a
                message describing the specific violation.
        """
        for query in queries:
            self.log.debug(
                "validate_query | Validating query (first 120 chars): '%.120s'", query
            )

            if not query or not query.strip():
                self.log.error("validate_query | Empty query detected.")
                raise AirflowException("Query must not be empty.")

            if query.count("/*") != query.count("*/"):
                self.log.error(
                    "validate_query | Unclosed block comment in query: '%.120s'", query
                )
                raise AirflowException("Unclosed block comment in query.")

            stripped = self._BLOCK_COMMENT_RE.sub("", query)
            stripped = self._LINE_COMMENT_RE.sub("", stripped)
            self.log.debug("validate_query | Stripped query: '%.120s'", stripped)

            if ";" in stripped:
                self.log.error(
                    "validate_query | Semicolon detected in query: '%.120s'", stripped
                )
                raise AirflowException("Semicolons are not allowed in queries.")

            if not re.match(r"^\s*select\b", stripped, re.IGNORECASE):
                self.log.error(
                    "validate_query | Non-SELECT query detected: '%.120s'", stripped
                )
                raise AirflowException("Only SELECT queries are allowed.")

            match = self._PROHIBITED_SQL_WORDS.search(stripped)
            if match:
                self.log.error(
                    "validate_query | Prohibited keyword '%s' detected in query: "
                    "'%.120s'",
                    match.group(),
                    stripped,
                )
                raise AirflowException(
                    f"Prohibited SQL keyword '{match.group()}' detected in query."
                )

            self.log.debug("validate_query | Query passed all validation checks.")

    # ------------------------------------------------------------------ #
    # Result builder                                                       #
    # ------------------------------------------------------------------ #

    def _build_result(
        self, test: Dict[str, Any], passed: bool = False
    ) -> Dict[str, Any]:
        """Construct a standardised result dictionary for a test.

        Args:
            test (Dict[str, Any]): The test definition dictionary.
            passed (bool): Whether the test passed. Defaults to ``False``.

        Returns:
            Dict[str, Any]: A dictionary with the following keys:
                - ``test_name`` (str)
                - ``status`` (bool) — ``True`` if passed, ``False`` otherwise
                - ``test_interval`` (int) — run interval in minutes
                - ``criticality`` (str)
                - ``source_database`` (str)
                - ``source_table`` (str)
                - ``test_type_key`` (str)
        """
        result = {
            "test_name": test.get("test_name"),
            "status": passed,
            "test_interval": int(test.get("interval_minutes", 0)),
            "criticality": test.get("criticality"),
            "source_database": test.get("source_database"),
            "source_table": test.get("source_table"),
            "test_type_key": test.get("test_type_key"),
        }
        self.log.debug(
            "_build_result | Built result for test='%s': %s",
            test.get("test_name"),
            result,
        )
        return result