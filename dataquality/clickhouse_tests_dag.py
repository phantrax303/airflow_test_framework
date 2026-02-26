from datetime import timedelta

from airflow.decorators import task, dag
import pendulum
from airflow.utils.session import provide_session
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import get_current_context
from utils.delete_xcom import delete_xcom_function
from utils.slack_alert import failure_callback_slack
from utils.TestOperator import TestOperator


num_worker_tasks = 3
num_thread_per_worker=3
SCHEDULE_CRON = f'*/15 * * * *'
DAG_ID = 'clickhouse_tests_dag'
src_database = 'internal_engineering'
src_table = 'config_tests_tbl'

def clickhouseToDict(rows:list, columns:list =None):
    
    tests_list = []
    for row in rows:
        test_dict = {}
        for column_pos in range(len(columns)):
            test_dict[columns[column_pos]] = row[column_pos]

        tests_list.append(test_dict)

    return  tests_list

@dag(
    dag_id=DAG_ID,
    schedule_interval=SCHEDULE_CRON,
    start_date=pendulum.datetime(2025, 8, 19, tz='UTC'),
    catchup=False,
    tags=['data quality'],
    description=f'DAG to run pre tests and tests every 15 minutes',
    on_failure_callback = lambda context: failure_callback_slack(
        context
    )
)
def alerts_dags():

    @task()
    def setup_test_databases():
        from utils.databases import ensure_databases
        ensure_databases('datapipe_clickhouse_cloud',src_config_db=src_database, src_config_tbl=src_table,dst_database='internal_engineering', dst_result_table='test_results')
        return True

    @task()
    def get_active_tests(num_workers):
        import clickhouse_connect
        from airflow.hooks.base import BaseHook

        context = get_current_context()
        logical_date = context['data_interval_end'].in_timezone('UTC')
        conn = BaseHook.get_connection('datapipe_clickhouse_cloud')
        with clickhouse_connect.get_client(
                host=conn.host,
                port = conn.port,
                database=conn.schema,
                user=conn.login,
                password=conn.password
            ) as client:

            columns_sql = f"""
                    select name from system.columns where database = '{src_database}' and table = '{src_table}' order by position
                """
            columns = client.query(columns_sql).result_rows
            columns = [column[0] for column in columns]
            sql = f"""
                    SELECT 
                        {','.join([f'`{column}`' for column in columns])}
                    FROM {src_database}.{src_table} final
                    where is_active = 1 
                        and 
                    (toHour(toDateTime('{logical_date.format('YYYY-MM-DD HH:mm:ss')}')) * 60 + toMinute(toDateTime('{logical_date.format('YYYY-MM-DD HH:mm:ss')}'))) % interval_minutes = `offset`
                """
            test = clickhouseToDict(client.query(sql).result_rows,columns)

        k, m = divmod(len(test), num_workers)
        print(f"Dividido em {num_workers} lotes para processamento.")
        return [
            test[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] 
            for i in range(num_workers)
        ]
    
    
    @task(trigger_rule=TriggerRule.ALL_DONE)
    def slack_report(test_batches):
        from utils.slack_alert import slack_alert
        
        failed_tests = []
        for batch in test_batches:
            if batch: 
                failed_tests.extend(batch)

        failed_tests = [test for test in failed_tests if test.get('passed') == False]

        if len(failed_tests) <= 0:
            return None
        
        failed_tests_names = '\n'.join([test['test_name'] for test in failed_tests])
        failed_tests_intervals = '\n'.join([f"{test['test_interval']} minutes" for test in failed_tests])
        failed_tests_criticality = '\n'.join([test['criticality'] for test in failed_tests])
        failed_tests_tables = '\n'.join([f"{test['source_database']}.{test['source_table']}" for test in failed_tests])
        failed_tests_types = '\n'.join([test['test_type_key'] for test in failed_tests])
        failed_tests_errors = '\n'.join([test['error'] for test in failed_tests])
        
        payload = {
            "text": f"Failed Tests",
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "Failed Tests Summary",
                        "emoji": True
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Failed Tests:*\n{failed_tests_names}"},
                    ]
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Test Intervals:*\n{failed_tests_intervals} "},
                    ]
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Criticallity:*\n{failed_tests_criticality}"},
                    ]
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Affected Tables:*\n{failed_tests_tables}"},
                    ]
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Test Errors:*\n{failed_tests_errors}"}
                    ]
                },
            ]
        }

        slack_alert(payload)

    @task(trigger_rule=TriggerRule.ALL_DONE)
    @provide_session
    def delete_xcom_task(session=None,):
        context = get_current_context()
        dag_run = context.get('dag_run')
        delete_xcom_function(dag_id=dag_run.dag_id, run_id = dag_run.run_id , session=session)

    setup = setup_test_databases()
    tests_lists = get_active_tests(num_worker_tasks)
    tests = TestOperator.partial(
        task_id='testing_task',
        conn_id = 'airflow_datapipe',
        num_workers=num_thread_per_worker,
        dst_database='internal_engineering',
        dst_result_table='result_tests_tbl'
    ).expand(test_list=tests_lists)

    alert = slack_report(tests)

    delete = delete_xcom_task()

    setup >> tests_lists >> tests >>alert >> delete

alerts_dags()