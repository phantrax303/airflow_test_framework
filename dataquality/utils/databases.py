from airflow.hooks.base import BaseHook
import logging
from airflow.exceptions import AirflowException

def ensure_databases(conn_id,src_config_db='default', src_config_tbl='test_config_tbl',dst_database='default', dst_result_table='test_results') -> bool:
        conn = BaseHook.get_connection(conn_id)
        if not conn:
            raise AirflowException(f"Connection with id {conn_id} not found.")
        if conn.conn_type == 'generic':
            conn_type = conn.extra_dejson.get('conn_type')
            if not conn_type:
                raise AirflowException(f"Connection {conn_id} is of type 'generic' but does not specify 'conn_type' in extras.")
        else:
            conn_type = conn.conn_type
        validate_identifier(src_config_db, src_config_tbl, dst_database, dst_result_table, conn_id,conn_type)
        ensure_func = ensure_selector(conn_type)
        try:
            src = ensure_func(conn_id, src_config_db, src_config_tbl,table_type='config')
        except Exception as e:
            logging.error(f"Error ensuring source database/table: {e}")
            src = False
        try:
            dst = ensure_func(conn_id, dst_database, dst_result_table,table_type='result')
        except Exception as e:
            logging.error(f"Error ensuring destination database/table: {e}")
            dst = False

        return src and dst

def ensure_selector(conn_type):
    func_map=   {'postgres': ensure_postgres,
                 'mysql': ensure_mysql,
                 'clickhouse': ensure_clickhouse}
    if conn_type not in func_map:
        raise ValueError(f"Unsupported connection type: {conn_type}")
    return func_map.get(conn_type)

def ensure_postgres(conn_id, database, table,table_type='config'):
    try:
        import psycopg2
    except Exception as e:
        raise ImportError("psycopg2 is required to use ensure_postgres function. Please install it using 'pip install psycopg2-binary'") from e
    conn_data = BaseHook.get_connection(conn_id)
    with psycopg2.connect(
                host=conn_data.host,
                port = conn_data.port,
                database=conn_data.schema,
                user=conn_data.login,
                password=conn_data.password
            ) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT table_schema, table_name from information_schema.tables where table_type = 'BASE TABLE' and has_table_privilege(table_schema||'.'||table_name, 'SELECT') and table_schema = 'public';")
            results = cur.fetchall()

def ensure_mysql(conn_id, database, table,table_type='config'):
    pass

def ensure_clickhouse(conn_id, database, table,table_type='config'):
    try:
        import clickhouse_connect
    except Exception as e:
        logging.error("clickhouse_connect library is not installed. Please install it using 'pip install clickhouse-connect'")
        raise ImportError("clickhouse_connect is required to use ensure_clickhouse function. Please install it using 'pip install clickhouse-connect'") from e
    conn_data = BaseHook.get_connection(conn_id)
    with clickhouse_connect.get_client(
                host=conn_data.host,
                port = conn_data.port,
                database=conn_data.schema,
                user=conn_data.login,
                password=conn_data.password
            ) as client:
        try:
            result = client.query(f"SELECT name FROM system.databases WHERE name = '{database}'")
            if len(result.result_rows) == 0:
                client.command(f"Create database {database}")
            else:
                logging.info(f"Database {database} already exists")
        except Exception as e   :
            logging.error(f"Error creating database {database}: {e}")  
            return False

        if table_type == 'config':

            ## Define the schema for the configuration table
            ## the configuration table is the base of our testing framework.
            ## the testing framework works in a two step process, first it runs the pre_test_query for each test, then it runs the test_query and compares the results with the defined thresholds and operators based on strategy.
            ## Example: if the strategy is ROWS, it will compare the number of rows returned by the test_query with the threshold using the defined operator. If the strategy is COUNT, it will compare the count of a specific column with the threshold using the defined operator.
            ## the sensitivity_columns field is used to dynamically replace columns in the test_query for sensitivity analysis. For example, if we want to run the same test for different columns, we can define the test_query with a placeholder for the column and then replace it with the actual column name from the sensitivity_columns field.
            columns = [('test_name', 'LowCardinality(String)'),
                        ('is_active', 'Bool DEFAULT true'),
                       ('test_interval_min', 'UInt32'),
                       ('offset', 'UInt16 DEFAULT 0'),
                       ('test_type', 'LowCardinality(String)'),
                       ('source_database', 'LowCardinality(String)'),
                       ('source_table', 'LowCardinality(String)'),
                       ('pre_test_params', 'Json'),
                       ('pre_test_query', 'String'),
                       ('expectation_concatenator',"Enum8('AND' = 0, 'OR' = 1)"),
                       ('expectations', 'Array(JSON)', 'comment "Array of JSON objects defining the expectations for the test.The following fields are obrigatory for each expectation: column, operator, value"'),
                       ('criticality', "Enum8('Low' = 0, 'Medium' = 1, 'High' = 2, 'Critical' = 3)"),
                       ('description', 'String')]

            schema = "(" + ",".join([f"{col[0]} {col[1]} {col[2] if len(col) > 2 else ''}" for col in columns]) + ") Engine=MergeTree() order by test_name"
            
        elif table_type == 'result':
            columns = [('test_time_window', 'DateTime'),
                        ('interval_minutes', 'UInt16'),
                        ('test_case', 'String'),
                        ('results', 'Json')]
            schema = "(" + ",".join([f"{col[0]} {col[1]}" for col in columns]) + ") Engine=MergeTree() order by (test_case, test_time_window) ttl test_time_window + INTERVAL 30 DAY"

        try:
            result = client.query(f"SELECT name FROM system.tables WHERE database = '{database}' AND name = '{table}'")
            if len(result.result_rows) == 0:
                client.command(f"Create table if not exists {database}.{table} {schema}")
            else:
                logging.info(f"Table {database}.{table} already exists")
                schema_result = client.query(f"select name, type from system.columns where database = '{database}' and table = '{table}' order by position").result_rows
                existing_schema = [(row[0], row[1]) for row in schema_result]
                if not all(existing_schema[pos][0] == column[0] and existing_schema[pos][1] == column[1] for pos, column in enumerate(columns) if pos < len(existing_schema)):
                    logging.warning(f"Schema mismatch for table {database}.{table}. Expected: {columns}, Existing: {existing_schema}")  
                    return False

        except Exception as e:
            logging.error(f"Error creating table {database}.{table}: {e}")
            return False

        return True

def validate_identifier(*identifiers: str) -> None:
    import re
    _IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_\.]+$")

    for val in identifiers:
        if not val or not _IDENTIFIER_RE.match(val):
            raise AirflowException(
                f"Invalid identifier '{val}'. Only alphanumeric characters, underscores and dots are allowed."
            )