"""
clickhouse_tests_dag — Airflow DAG for running periodic ClickHouse data-quality tests.

Overview
--------
This DAG is the orchestration layer of the data-quality testing framework.
Every **15 minutes** it:

1. **Sets up** the required ClickHouse databases and tables (config + results) via
   :func:`~utils.databases.ensure_databases`.
2. **Fetches** all active test definitions whose scheduled interval aligns with the
   current UTC time window.
3. **Distributes** the test list across ``num_worker_tasks`` dynamic task instances
   using :class:`~utils.TestOperator.TestOperator` (each instance runs
   ``num_thread_per_worker`` threads internally).
4. **Alerts** via Slack on any failed test, grouped into a single summary message.
5. **Cleans up** XCom entries produced during the run.

Configuration
-------------
``num_worker_tasks`` (int)
    Number of parallel ``TestOperator`` task instances (dynamic map expand).
    Increase to scale out across more Airflow workers.

``num_thread_per_worker`` (int)
    Thread-level parallelism *within* each ``TestOperator`` instance.
    Each thread maintains its own ClickHouse connection.

``SCHEDULE_CRON`` (str)
    Cron expression controlling how often the DAG fires.
    Default: ``*/15 * * * *`` (every 15 minutes).

``src_database`` / ``src_table`` (str)
    ClickHouse location of the test-configuration table that is read at runtime.

Task graph
----------
::

    setup_test_databases
           │
    get_active_tests
           │
    testing_task (mapped × num_worker_tasks)
           │
      slack_report
           │
    delete_xcom_task
"""

from datetime import timedelta

from airflow.decorators import task, dag
import pendulum
import logging
from airflow.utils.session import provide_session
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import get_current_context
from utils.delete_xcom import delete_xcom_function
from utils.slack_alert import failure_callback_slack
from utils.TestOperator import TestOperator


# ── DAG-level constants ──────────────────────────────────────────────────────

#: Number of parallel ``TestOperator`` task instances created via dynamic mapping.
num_worker_tasks = 3

#: Number of concurrent threads per ``TestOperator`` instance.
num_thread_per_worker = 3

#: Cron schedule — fires every 15 minutes.
SCHEDULE_CRON = "*/15 * * * *"

#: Airflow DAG identifier.
DAG_ID = "clickhouse_tests_dag"

#: ClickHouse database that holds the test-configuration table.
src_database = "internal_engineering"

#: ClickHouse table that holds the test definitions.
src_table = "config_tests_tbl"


# ── Helper functions ─────────────────────────────────────────────────────────

def clickhouseToDict(rows: list, columns: list = None) -> list:
    """Convert ClickHouse query result rows into a list of dictionaries.

    Each row from ``clickhouse_connect`` is returned as a plain tuple.  This
    function zips column names with the corresponding values so that downstream
    code can access fields by name.

    Args:
        rows (list): List of tuples returned by a ClickHouse query
            (i.e., ``client.query(...).result_rows``).
        columns (list): Ordered list of column name strings corresponding to
            the positions in each row tuple.

    Returns:
        list[dict]: A list of dictionaries, one per row, where keys are column
        names and values are the raw column values.

    Example::

        rows    = [(1, 'my_test', True), (2, 'other_test', False)]
        columns = ['id', 'test_name', 'is_active']
        result  = clickhouseToDict(rows, columns)
        # → [{'id': 1, 'test_name': 'my_test', 'is_active': True}, …]
    """
    logging.debug(
        "clickhouseToDict | Converting %d row(s) with %d column(s).",
        len(rows),
        len(columns) if columns else 0,
    )
    tests_list = []
    for row in rows:
        test_dict = {}
        for column_pos in range(len(columns)):
            test_dict[columns[column_pos]] = row[column_pos]
        tests_list.append(test_dict)

    logging.debug("clickhouseToDict | Produced %d dict(s).", len(tests_list))
    return tests_list


# ── DAG definition ───────────────────────────────────────────────────────────

@dag(
    dag_id=DAG_ID,
    schedule_interval=SCHEDULE_CRON,
    start_date=pendulum.datetime(2025, 8, 19, tz="UTC"),
    catchup=False,
    tags=["data quality"],
    description="DAG to run pre tests and tests every 15 minutes",
    on_failure_callback=lambda context: failure_callback_slack(context),
)
def alerts_dags():
    """Define the ``clickhouse_tests_dag`` task graph.

    All tasks are defined as nested functions so they can share module-level
    constants (``src_database``, ``src_table``, etc.) without passing them
    explicitly through XCom.

    Task descriptions
    -----------------
    ``setup_test_databases``
        Ensures the ClickHouse config and result tables exist before any test
        logic runs.  Fails the DAG early if provisioning is unsuccessful.

    ``get_active_tests``
        Queries the config table for tests that are *active* **and** whose
        scheduling interval aligns with the current time window.  Splits the
        results into ``num_worker_tasks`` evenly-sized batches for parallel
        processing.

    ``testing_task`` (dynamically mapped)
        Executes the tests in each batch using :class:`~utils.TestOperator.TestOperator`.
        Each mapped instance runs up to ``num_thread_per_worker`` threads.
        Trigger rule is inherited from the operator default (``ALL_SUCCESS``).

    ``slack_report``
        Collects results from all mapped ``testing_task`` instances, filters for
        failures, and posts a single formatted Slack summary.  Uses
        ``ALL_DONE`` trigger so it always runs regardless of upstream failures.

    ``delete_xcom_task``
        Removes all XCom entries for the current ``dag_run`` to prevent storage
        bloat.  Uses ``ALL_DONE`` trigger to always execute as the final step.
    """

    @task()
    def setup_test_databases():
        """Provision ClickHouse databases and tables required by the framework.

        Delegates to :func:`~utils.databases.ensure_databases` which handles
        database/table creation and schema validation for both the config source
        table and the results destination table.

        Returns:
            bool: ``True`` when both tables are confirmed/created successfully.
                Returning a value allows Airflow to push it to XCom and lets
                downstream tasks declare this task as a dependency.

        Raises:
            Exception: Any unhandled exception propagates and marks the task as
                failed, which will trigger the DAG-level ``on_failure_callback``.
        """
        logging.info(
            "setup_test_databases | Starting database/table provisioning | "
            "config=%s.%s | result=%s.%s",
            src_database,
            src_table,
            "internal_engineering",
            "test_results",
        )
        from utils.databases import ensure_databases

        result = ensure_databases(
            "datapipe_clickhouse_cloud",
            src_config_db=src_database,
            src_config_tbl=src_table,
            dst_database="internal_engineering",
            dst_result_table="test_results",
        )
        if result:
            logging.info("setup_test_databases | Provisioning completed successfully.")
        else:
            logging.warning(
                "setup_test_databases | Provisioning returned False — one or more "
                "tables may not be ready."
            )
        return result

    @task()
    def get_active_tests(num_workers: int) -> list:
        """Fetch active test definitions and split them into worker batches.

        Connects to ClickHouse using the ``datapipe_clickhouse_cloud`` Airflow
        connection.  Reads column names dynamically from ``system.columns`` to
        remain resilient to schema additions, then SELECTs all active tests whose
        scheduling offset aligns with the current UTC ``data_interval_end``.

        The result is partitioned into ``num_workers`` roughly equal lists so that
        the dynamic-mapped ``TestOperator`` tasks receive balanced workloads.

        Args:
            num_workers (int): Desired number of output batches (one per mapped
                ``TestOperator`` instance).

        Returns:
            list[list[dict]]: A list of ``num_workers`` sub-lists.  Each sub-list
            contains test-definition dicts for one ``TestOperator`` instance.
            Empty sub-lists are possible when the total number of active tests is
            smaller than ``num_workers``.

        Notes:
            The scheduling filter uses the expression::

                (toHour(logical_date) * 60 + toMinute(logical_date)) % interval_minutes = offset

            This ensures each test fires at a predictable sub-hourly cadence even
            when the DAG runs every 15 minutes.
        """
        import clickhouse_connect
        from airflow.hooks.base import BaseHook

        context = get_current_context()
        logical_date = context["data_interval_end"].in_timezone("UTC")
        logging.info(
            "get_active_tests | Fetching active tests | logical_date='%s' | "
            "num_workers=%d",
            logical_date.format("YYYY-MM-DD HH:mm:ss"),
            num_workers,
        )

        conn = BaseHook.get_connection("datapipe_clickhouse_cloud")
        logging.debug(
            "get_active_tests | ClickHouse connection | host='%s' port=%s",
            conn.host,
            conn.port,
        )

        with clickhouse_connect.get_client(
            host=conn.host,
            port=conn.port,
            database=conn.schema,
            user=conn.login,
            password=conn.password,
        ) as client:

            # ── Fetch column names dynamically ────────────────────────
            columns_sql = (
                f"select name from system.columns "
                f"where database = '{src_database}' and table = '{src_table}' "
                f"order by position"
            )
            logging.debug(
                "get_active_tests | Fetching column names | query: %s", columns_sql
            )
            columns = client.query(columns_sql).result_rows
            columns = [column[0] for column in columns]
            logging.info(
                "get_active_tests | Retrieved %d column(s) from '%s.%s': %s",
                len(columns),
                src_database,
                src_table,
                columns,
            )

            # ── Fetch active tests for current time window ────────────
            formatted_date = logical_date.format("YYYY-MM-DD HH:mm:ss")
            sql = f"""
                    SELECT
                        {','.join([f'`{column}`' for column in columns])}
                    FROM {src_database}.{src_table} final
                    where is_active = 1
                        and
                    (toHour(toDateTime('{formatted_date}')) * 60 + toMinute(toDateTime('{formatted_date}'))) % interval_minutes = `offset`
                """
            logging.debug(
                "get_active_tests | Active-tests query: %s", sql.strip()
            )
            test = clickhouseToDict(client.query(sql).result_rows, columns)
            logging.info(
                "get_active_tests | %d active test(s) scheduled for this window.",
                len(test),
            )

        # ── Partition into worker batches ─────────────────────────────
        k, m = divmod(len(test), num_workers)
        batches = [
            test[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)]
            for i in range(num_workers)
        ]
        batch_sizes = [len(b) for b in batches]
        logging.info(
            "get_active_tests | Split %d test(s) into %d batch(es): sizes=%s",
            len(test),
            num_workers,
            batch_sizes,
        )
        return batches

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def slack_report(test_batches: list) -> None:
        """Post a Slack summary of all failed tests from the current run.

        Collects result dicts from all mapped ``TestOperator`` instances, filters
        for items where ``passed == False``, and sends a formatted Block Kit message
        to the configured Slack webhook.  If there are no failures the function
        returns early without sending any message.

        Args:
            test_batches (list): Nested list of test-result dicts.  Each element
                corresponds to the return value of one ``TestOperator`` instance.
                A result dict is expected to contain ``test_name``,
                ``test_interval``, ``criticality``, ``source_database``,
                ``source_table``, ``test_type_key``, ``passed``, and optionally
                ``error``.

        Returns:
            None: This task does not push any value to XCom.
        """
        from utils.slack_alert import slack_alert

        logging.info(
            "slack_report | START | Received %d batch(es).", len(test_batches)
        )

        # Flatten batches and filter failures
        failed_tests = []
        for idx, batch in enumerate(test_batches):
            logging.debug(
                "slack_report | Batch %d contains %d result(s).",
                idx,
                len(batch) if batch else 0,
            )
            if batch:
                failed_tests.extend(batch)

        failed_tests = [t for t in failed_tests if t.get("passed") is False]
        logging.info(
            "slack_report | Found %d failed test(s) across all batches.",
            len(failed_tests),
        )

        if len(failed_tests) <= 0:
            logging.info("slack_report | No failed tests — skipping Slack alert.")
            return None

        # Build alert payload
        failed_tests_names = "\n".join([t["test_name"] for t in failed_tests])
        failed_tests_intervals = "\n".join(
            [f"{t['test_interval']} minutes" for t in failed_tests]
        )
        failed_tests_criticality = "\n".join([t["criticality"] for t in failed_tests])
        failed_tests_tables = "\n".join(
            [f"{t['source_database']}.{t['source_table']}" for t in failed_tests]
        )
        failed_tests_types = "\n".join([t["test_type_key"] for t in failed_tests])
        failed_tests_errors = "\n".join([t.get("error", "N/A") for t in failed_tests])

        logging.debug(
            "slack_report | Preparing Slack payload | failed_tests_names: %s",
            failed_tests_names,
        )

        payload = {
            "text": "Failed Tests",
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "Failed Tests Summary",
                        "emoji": True,
                    },
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Failed Tests:*\n{failed_tests_names}"},
                    ],
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Test Intervals:*\n{failed_tests_intervals} "},
                    ],
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Criticallity:*\n{failed_tests_criticality}"},
                    ],
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Affected Tables:*\n{failed_tests_tables}"},
                    ],
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Test Errors:*\n{failed_tests_errors}"}
                    ],
                },
            ],
        }

        logging.info(
            "slack_report | Sending Slack alert for %d failure(s).", len(failed_tests)
        )
        slack_alert(payload)
        logging.info("slack_report | Slack alert sent successfully.")

    @task(trigger_rule=TriggerRule.ALL_DONE)
    @provide_session
    def delete_xcom_task(session=None):
        """Delete all XCom entries produced during the current DAG run.

        This is a housekeeping task that runs last (``ALL_DONE`` trigger) to
        prevent XCom table bloat caused by large test-result payloads being
        stored across many dynamic task instances.

        Args:
            session: SQLAlchemy session injected by the ``@provide_session``
                decorator.  Do not pass explicitly.

        Returns:
            None
        """
        context = get_current_context()
        dag_run = context.get("dag_run")
        logging.info(
            "delete_xcom_task | Deleting XCom entries | dag_id='%s' | run_id='%s'",
            dag_run.dag_id,
            dag_run.run_id,
        )
        delete_xcom_function(
            dag_id=dag_run.dag_id, run_id=dag_run.run_id, session=session
        )
        logging.info(
            "delete_xcom_task | XCom cleanup completed | dag_id='%s' | run_id='%s'",
            dag_run.dag_id,
            dag_run.run_id,
        )

    # ── Wire up the task graph ───────────────────────────────────────────
    setup = setup_test_databases()
    tests_lists = get_active_tests(num_worker_tasks)
    tests = TestOperator.partial(
        task_id="testing_task",
        conn_id="airflow_datapipe",
        num_workers=num_thread_per_worker,
        dst_database="internal_engineering",
        dst_result_table="result_tests_tbl",
    ).expand(test_list=tests_lists)

    alert = slack_report(tests)
    delete = delete_xcom_task()

    setup >> tests_lists >> tests >> alert >> delete


alerts_dags()