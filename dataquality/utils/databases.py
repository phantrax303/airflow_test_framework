"""
databases module — database and table provisioning utilities for the testing framework.

This module provides helper functions to ensure that the required ClickHouse (and
optionally PostgreSQL / MySQL) databases and tables exist before any test DAG run.

The main entry-point is :func:`ensure_databases`, which is called by the
``setup_test_databases`` Airflow task in ``clickhouse_tests_dag``.  It dispatches
to the appropriate engine-specific function based on the Airflow connection type.

Supported connection types
--------------------------
- ``clickhouse`` — full implementation via :func:`ensure_clickhouse`.
- ``postgres``   — partial stub via :func:`ensure_postgres` (SELECT privilege check).
- ``mysql``      — placeholder via :func:`ensure_mysql` (no-op, returns ``None``).

Table schemas managed
---------------------
config table
    Stores test definitions consumed by :class:`~utils.TestOperator.TestOperator`.
    Key columns: ``test_name``, ``is_active``, ``test_interval_min``, ``offset``,
    ``test_type``, ``source_database``, ``source_table``, ``pre_test_params``,
    ``pre_test_query``, ``expectation_concatenator``, ``expectations``,
    ``criticality``, ``description``.

result table
    Stores per-run metric snapshots written by the pre-test query step.
    Key columns: ``test_time_window``, ``interval_minutes``, ``test_case``,
    ``results``.  Rows are automatically expired after 30 days via a TTL.
"""

from airflow.hooks.base import BaseHook
import logging
from airflow.exceptions import AirflowException


def ensure_databases(
    conn_id: str,
    src_config_db: str = "default",
    src_config_tbl: str = "test_config_tbl",
    dst_database: str = "default",
    dst_result_table: str = "test_results",
) -> bool:
    """Ensure that both the config source table and the result destination table exist.

    This is the main entry-point called by the ``setup_test_databases`` Airflow task.
    It resolves the correct engine-specific function based on the Airflow connection
    type and calls it for both the config table and the result table.

    Steps performed:

    1. Look up the Airflow connection identified by ``conn_id``.
    2. For ``generic`` connections, resolve the real ``conn_type`` from extras.
    3. Validate all identifier arguments to prevent SQL injection.
    4. Select and invoke the appropriate ``ensure_*`` function.
    5. Return ``True`` only if *both* the config and result ensure calls succeed.

    Args:
        conn_id (str): Airflow connection ID to use.
        src_config_db (str): Database containing the test-configuration table.
            Defaults to ``'default'``.
        src_config_tbl (str): Name of the test-configuration table.
            Defaults to ``'test_config_tbl'``.
        dst_database (str): Database where test results should be stored.
            Defaults to ``'default'``.
        dst_result_table (str): Name of the test-results table.
            Defaults to ``'test_results'``.

    Returns:
        bool: ``True`` if both tables were confirmed/created successfully,
        ``False`` if either step failed (errors are logged but not re-raised).

    Raises:
        AirflowException: If the connection is not found, if a ``generic``
            connection is missing its ``conn_type`` extra, or if any identifier
            fails validation.
        ValueError: If the resolved ``conn_type`` is not supported.
    """
    logging.info(
        "ensure_databases | START | conn_id='%s' | "
        "src=%s.%s | dst=%s.%s",
        conn_id,
        src_config_db,
        src_config_tbl,
        dst_database,
        dst_result_table,
    )

    conn = BaseHook.get_connection(conn_id)
    if not conn:
        logging.error("ensure_databases | Connection '%s' not found.", conn_id)
        raise AirflowException(f"Connection with id {conn_id} not found.")

    logging.debug(
        "ensure_databases | Connection found | conn_id='%s' | raw_type='%s'",
        conn_id,
        conn.conn_type,
    )

    if conn.conn_type == "generic":
        conn_type = conn.extra_dejson.get("conn_type")
        if not conn_type:
            logging.error(
                "ensure_databases | Generic connection '%s' is missing 'conn_type' extra.",
                conn_id,
            )
            raise AirflowException(
                f"Connection {conn_id} is of type 'generic' but does not specify "
                "'conn_type' in extras."
            )
        logging.debug(
            "ensure_databases | Resolved generic conn_type='%s' for conn_id='%s'.",
            conn_type,
            conn_id,
        )
    else:
        conn_type = conn.conn_type

    logging.info(
        "ensure_databases | Effective connection type: '%s' | conn_id='%s'",
        conn_type,
        conn_id,
    )

    validate_identifier(src_config_db, src_config_tbl, dst_database, dst_result_table, conn_id, conn_type)
    logging.debug("ensure_databases | All identifiers passed validation.")

    ensure_func = ensure_selector(conn_type)
    logging.debug(
        "ensure_databases | Selected ensure function: '%s' for conn_type='%s'.",
        ensure_func.__name__,
        conn_type,
    )

    # ── Config table ────────────────────────────────────────────────────
    logging.info(
        "ensure_databases | Ensuring config table '%s.%s'.",
        src_config_db,
        src_config_tbl,
    )
    try:
        src = ensure_func(conn_id, src_config_db, src_config_tbl, table_type="config")
        logging.info(
            "ensure_databases | Config table ensure result: %s | table='%s.%s'",
            src,
            src_config_db,
            src_config_tbl,
        )
    except Exception as exc:
        logging.error(
            "ensure_databases | Error ensuring config table '%s.%s': %s",
            src_config_db,
            src_config_tbl,
            exc,
        )
        src = False

    # ── Result table ────────────────────────────────────────────────────
    logging.info(
        "ensure_databases | Ensuring result table '%s.%s'.",
        dst_database,
        dst_result_table,
    )
    try:
        dst = ensure_func(conn_id, dst_database, dst_result_table, table_type="result")
        logging.info(
            "ensure_databases | Result table ensure result: %s | table='%s.%s'",
            dst,
            dst_database,
            dst_result_table,
        )
    except Exception as exc:
        logging.error(
            "ensure_databases | Error ensuring result table '%s.%s': %s",
            dst_database,
            dst_result_table,
            exc,
        )
        dst = False

    overall = src and dst
    logging.info(
        "ensure_databases | END | overall_success=%s | src=%s | dst=%s",
        overall,
        src,
        dst,
    )
    return overall


def ensure_selector(conn_type: str):
    """Return the appropriate ``ensure_*`` function for the given connection type.

    Args:
        conn_type (str): A supported connection type string.
            Currently recognised values: ``'postgres'``, ``'mysql'``,
            ``'clickhouse'``.

    Returns:
        Callable: The matching ``ensure_*`` function.

    Raises:
        ValueError: If ``conn_type`` is not in the supported map.
    """
    logging.debug("ensure_selector | Resolving function for conn_type='%s'.", conn_type)
    func_map = {
        "postgres": ensure_postgres,
        "mysql": ensure_mysql,
        "clickhouse": ensure_clickhouse,
    }
    if conn_type not in func_map:
        logging.error(
            "ensure_selector | Unsupported connection type: '%s'. "
            "Supported types: %s",
            conn_type,
            list(func_map.keys()),
        )
        raise ValueError(f"Unsupported connection type: {conn_type}")
    logging.debug(
        "ensure_selector | Resolved '%s' → '%s'.",
        conn_type,
        func_map[conn_type].__name__,
    )
    return func_map.get(conn_type)


def ensure_postgres(conn_id: str, database: str, table: str, table_type: str = "config") -> None:
    """Check SELECT privilege on all public BASE TABLEs in a PostgreSQL database.

    .. note::
        This is a **partial implementation**.  It currently only verifies that
        the connection works and lists accessible tables.  It does not create
        the config or result schema.  A ``None`` / implicit ``None`` return
        indicates the check ran without raising an exception.

    Args:
        conn_id (str): Airflow connection ID for the PostgreSQL instance.
        database (str): Target database name (informational; not used to switch
            databases — psycopg2 uses the schema from the connection).
        table (str): Target table name (informational; not yet used in checks).
        table_type (str): One of ``'config'`` or ``'result'``. Defaults to
            ``'config'``.

    Raises:
        ImportError: If ``psycopg2`` is not installed.
    """
    logging.info(
        "ensure_postgres | START | conn_id='%s' | database='%s' | table='%s' | "
        "table_type='%s'",
        conn_id,
        database,
        table,
        table_type,
    )
    try:
        import psycopg2
    except Exception as exc:
        logging.error(
            "ensure_postgres | psycopg2 not available: %s", exc
        )
        raise ImportError(
            "psycopg2 is required to use ensure_postgres function. "
            "Please install it using 'pip install psycopg2-binary'"
        ) from exc

    conn_data = BaseHook.get_connection(conn_id)
    logging.debug(
        "ensure_postgres | Connecting to PostgreSQL | host='%s' port=%s",
        conn_data.host,
        conn_data.port,
    )
    with psycopg2.connect(
        host=conn_data.host,
        port=conn_data.port,
        database=conn_data.schema,
        user=conn_data.login,
        password=conn_data.password,
    ) as conn:
        with conn.cursor() as cur:
            logging.debug("ensure_postgres | Querying information_schema.tables for accessible tables.")
            cur.execute(
                "SELECT table_schema, table_name "
                "FROM information_schema.tables "
                "WHERE table_type = 'BASE TABLE' "
                "  AND has_table_privilege(table_schema||'.'||table_name, 'SELECT') "
                "  AND table_schema = 'public';"
            )
            results = cur.fetchall()
            logging.info(
                "ensure_postgres | Found %d accessible table(s) in public schema.",
                len(results),
            )
    logging.info("ensure_postgres | END | conn_id='%s'", conn_id)


def ensure_mysql(conn_id: str, database: str, table: str, table_type: str = "config") -> None:
    """Placeholder for MySQL database/table provisioning (not yet implemented).

    Args:
        conn_id (str): Airflow connection ID for the MySQL instance.
        database (str): Target database name.
        table (str): Target table name.
        table_type (str): One of ``'config'`` or ``'result'``. Defaults to
            ``'config'``.

    Returns:
        None: This function is a no-op placeholder.
    """
    logging.warning(
        "ensure_mysql | Called but not implemented | "
        "conn_id='%s' | database='%s' | table='%s' | table_type='%s'",
        conn_id,
        database,
        table,
        table_type,
    )


def ensure_clickhouse(
    conn_id: str, database: str, table: str, table_type: str = "config"
) -> bool:
    """Ensure a ClickHouse database and table exist, creating them if necessary.

    For an existing table the function also performs a **schema compatibility
    check** — it compares column names and types (by position) against the
    expected schema definition.  A mismatch is logged as a warning and the
    function returns ``False`` without altering the existing table.

    Schema definitions
    ------------------
    *config* table (``table_type='config'``):
        Stores test definitions.  Uses ``MergeTree`` ordered by ``test_name``.

    *result* table (``table_type='result'``):
        Stores per-run metric snapshots.  Uses ``MergeTree`` ordered by
        ``(test_case, test_time_window)`` with a 30-day TTL on
        ``test_time_window``.

    Args:
        conn_id (str): Airflow connection ID for the ClickHouse instance.
        database (str): ClickHouse database to check/create.
        table (str): ClickHouse table to check/create within ``database``.
        table_type (str): Schema variant to apply — ``'config'`` or
            ``'result'``. Defaults to ``'config'``.

    Returns:
        bool: ``True`` if the database and table are confirmed or created
        successfully with a compatible schema; ``False`` on any error or
        schema mismatch.

    Raises:
        ImportError: If ``clickhouse_connect`` is not installed.
    """
    logging.info(
        "ensure_clickhouse | START | conn_id='%s' | database='%s' | "
        "table='%s' | table_type='%s'",
        conn_id,
        database,
        table,
        table_type,
    )

    try:
        import clickhouse_connect
    except Exception as exc:
        logging.error(
            "ensure_clickhouse | clickhouse_connect not installed: %s", exc
        )
        raise ImportError(
            "clickhouse_connect is required to use ensure_clickhouse function. "
            "Please install it using 'pip install clickhouse-connect'"
        ) from exc

    conn_data = BaseHook.get_connection(conn_id)
    logging.debug(
        "ensure_clickhouse | Connecting to ClickHouse | host='%s' port=%s",
        conn_data.host,
        conn_data.port,
    )

    with clickhouse_connect.get_client(
        host=conn_data.host,
        port=conn_data.port,
        database=conn_data.schema,
        user=conn_data.login,
        password=conn_data.password,
    ) as client:

        # ── Database ─────────────────────────────────────────────────
        logging.debug(
            "ensure_clickhouse | Checking if database '%s' exists.", database
        )
        try:
            result = client.query(
                f"SELECT name FROM system.databases WHERE name = '{database}'"
            )
            if len(result.result_rows) == 0:
                logging.info(
                    "ensure_clickhouse | Database '%s' not found. Creating…", database
                )
                client.command(f"Create database {database}")
                logging.info(
                    "ensure_clickhouse | Database '%s' created successfully.", database
                )
            else:
                logging.info(
                    "ensure_clickhouse | Database '%s' already exists.", database
                )
        except Exception as exc:
            logging.error(
                "ensure_clickhouse | Error ensuring database '%s': %s", database, exc
            )
            return False

        # ── Schema definition ─────────────────────────────────────────
        if table_type == "config":
            logging.debug(
                "ensure_clickhouse | Using 'config' schema for table '%s.%s'.",
                database,
                table,
            )
            columns = [
                ("test_name", "LowCardinality(String)"),
                ("is_active", "Bool DEFAULT true"),
                ("test_interval_min", "UInt32"),
                ("offset", "UInt16 DEFAULT 0"),
                ("test_type", "LowCardinality(String)"),
                ("source_database", "LowCardinality(String)"),
                ("source_table", "LowCardinality(String)"),
                ("pre_test_params", "Json"),
                ("pre_test_query", "String"),
                ("expectation_concatenator", "Enum8('AND' = 0, 'OR' = 1)"),
                (
                    "expectations",
                    "Array(JSON)",
                    'comment "Array of JSON objects defining the expectations for the test.'
                    'The following fields are obrigatory for each expectation: column, operator, value"',
                ),
                ("criticality", "Enum8('Low' = 0, 'Medium' = 1, 'High' = 2, 'Critical' = 3)"),
                ("description", "String"),
            ]
            schema = (
                "("
                + ",".join(
                    [
                        f"{col[0]} {col[1]} {col[2] if len(col) > 2 else ''}"
                        for col in columns
                    ]
                )
                + ") Engine=MergeTree() order by test_name"
            )

        elif table_type == "result":
            logging.debug(
                "ensure_clickhouse | Using 'result' schema for table '%s.%s'.",
                database,
                table,
            )
            columns = [
                ("test_time_window", "DateTime"),
                ("interval_minutes", "UInt16"),
                ("test_case", "String"),
                ("results", "Json"),
            ]
            schema = (
                "("
                + ",".join([f"{col[0]} {col[1]}" for col in columns])
                + ") Engine=MergeTree() order by (test_case, test_time_window) "
                "ttl test_time_window + INTERVAL 30 DAY"
            )
        else:
            logging.error(
                "ensure_clickhouse | Unknown table_type '%s' for table '%s.%s'.",
                table_type,
                database,
                table,
            )
            return False

        # ── Table ─────────────────────────────────────────────────────
        logging.debug(
            "ensure_clickhouse | Checking if table '%s.%s' exists.", database, table
        )
        try:
            result = client.query(
                f"SELECT name FROM system.tables "
                f"WHERE database = '{database}' AND name = '{table}'"
            )
            if len(result.result_rows) == 0:
                logging.info(
                    "ensure_clickhouse | Table '%s.%s' not found. Creating with "
                    "schema: %s",
                    database,
                    table,
                    schema,
                )
                client.command(
                    f"Create table if not exists {database}.{table} {schema}"
                )
                logging.info(
                    "ensure_clickhouse | Table '%s.%s' created successfully.",
                    database,
                    table,
                )
            else:
                logging.info(
                    "ensure_clickhouse | Table '%s.%s' already exists. "
                    "Performing schema compatibility check.",
                    database,
                    table,
                )
                schema_result = client.query(
                    f"select name, type from system.columns "
                    f"where database = '{database}' and table = '{table}' "
                    f"order by position"
                ).result_rows
                existing_schema = [(row[0], row[1]) for row in schema_result]
                logging.debug(
                    "ensure_clickhouse | Existing schema for '%s.%s': %s",
                    database,
                    table,
                    existing_schema,
                )

                schema_ok = all(
                    existing_schema[pos][0] == column[0]
                    and existing_schema[pos][1] == column[1]
                    for pos, column in enumerate(columns)
                    if pos < len(existing_schema)
                )

                if not schema_ok:
                    logging.warning(
                        "ensure_clickhouse | Schema MISMATCH for table '%s.%s'. "
                        "Expected columns: %s | Existing columns: %s",
                        database,
                        table,
                        [(c[0], c[1]) for c in columns],
                        existing_schema,
                    )
                    return False

                logging.info(
                    "ensure_clickhouse | Schema for '%s.%s' is compatible.",
                    database,
                    table,
                )

        except Exception as exc:
            logging.error(
                "ensure_clickhouse | Error creating/checking table '%s.%s': %s",
                database,
                table,
                exc,
            )
            return False

        logging.info(
            "ensure_clickhouse | END | conn_id='%s' | database='%s' | table='%s' | "
            "table_type='%s' | success=True",
            conn_id,
            database,
            table,
            table_type,
        )
        return True


def validate_identifier(*identifiers: str) -> None:
    """Validate that all provided identifiers are safe for SQL use.

    Each identifier is checked against ``^[A-Za-z0-9_\\.]+$``.  This prevents
    SQL injection through user-controlled database or table names.

    Args:
        *identifiers (str): One or more identifier strings to validate.

    Raises:
        AirflowException: If any identifier is ``None``, empty, or contains
            characters outside the allowed character set.
    """
    import re
    _IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_\.]+$")

    for val in identifiers:
        logging.debug("validate_identifier | Checking identifier: '%s'", val)
        if not val or not _IDENTIFIER_RE.match(val):
            logging.error(
                "validate_identifier | INVALID identifier: '%s'", val
            )
            raise AirflowException(
                f"Invalid identifier '{val}'. "
                "Only alphanumeric characters, underscores and dots are allowed."
            )
    logging.debug(
        "validate_identifier | All %d identifier(s) passed validation.",
        len(identifiers),
    )