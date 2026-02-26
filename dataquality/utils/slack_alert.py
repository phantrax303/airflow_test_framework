
from airflow.models import Variable
from typing import List, Optional, Dict, Any
from datetime import datetime
import logging

def slack_alert(payload, webhook_key="slack_webhook"):
    """
    Send alert to Slack using only the requests library.
    
    Args:
        payload: Dictionary with Slack payload (Block Kit format)
        webhook_key: Name of the Airflow variable containing the webhook URL
    """
    import requests
    import json
    
    
    webhook_url = Variable.get(webhook_key, default_var=None)

    if not webhook_url:
        logging.error(f"No valid webhook URL found for key '{webhook_key}'.")
        return

    response = requests.post(
        webhook_url, 
        data=json.dumps(payload),
        headers={'Content-Type': 'application/json'}
    )

    if response.status_code != 200:
        print(f"Error sending alert to Slack: {response.status_code} - {response.text}")


def format_user_mentions(users: List[str]) -> str:
    """
    Format user mentions for Slack.
    
    Args:
        users: List of user IDs (format: 'U123456' or '@user' or '<@U123456>')
        
    Returns:
        Formatted string with all mentions
    """
    mentions = []
    for user in users:
        if user.startswith('<@') and user.endswith('>'):
            # Already formatted
            mentions.append(user)
        elif user.startswith('@'):
            # Remove @ and add Slack format
            mentions.append(f"<@{user[1:]}>" if user[1:].startswith('U') else f"@{user[1:]}")
        elif user.startswith('U'):
            # Slack user ID
            mentions.append(f"<@{user}>")
        else:
            # Assume it's a simple username
            mentions.append(f"@{user}")
    return " ".join(mentions)


def build_slack_blocks(
    header_emoji: str,
    header_text: str,
    mention_users: Optional[List[str]] = None,
    mention_prefix: str = "📢 Attention:",
    custom_message: Optional[str] = None,
    fields: Optional[List[Dict[str, str]]] = None,
    content_blocks: Optional[List[Dict[str, Any]]] = None,
    buttons: Optional[List[Dict[str, Any]]] = None,
    divider: bool = False
) -> List[Dict[str, Any]]:
    """
    Build a list of Slack blocks dynamically.
    
    Args:
        header_emoji: Emoji for the header
        header_text: Text for the header
        mention_users: List of Slack user IDs to mention
        mention_prefix: Prefix text before user mentions (default: "📢 Attention:")
        custom_message: Optional custom message before details
        fields: List of field dictionaries to display (format: [{"type": "mrkdwn", "text": "*Label:*\nValue"}])
        content_blocks: Additional custom content blocks to add
        buttons: List of button configurations (format: [{"text": "Label", "url": "https://...", "style": "danger"}])
        divider: Add a divider at the end (default: False)
        
    Returns:
        List of Slack blocks ready to use in payload
    """
    blocks = []
    
    # Header block
    blocks.append({
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": f"{header_emoji} {header_text}",
            "emoji": True
        }
    })
    
    # User mentions block (if provided)
    if mention_users:
        mentions = format_user_mentions(mention_users)
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*{mention_prefix}* {mentions}"
            }
        })
    
    # Custom message block (if provided)
    if custom_message:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": custom_message
            }
        })
    
    # Fields block (if provided)
    if fields:
        blocks.append({
            "type": "section",
            "fields": fields
        })
    
    # Additional content blocks (if provided)
    if content_blocks:
        blocks.extend(content_blocks)
    
    # Buttons block (if provided)
    if buttons:
        elements = []
        for btn in buttons:
            button_config = {
                "type": "button",
                "text": {"type": "plain_text", "text": btn.get("text", "Click")},
                "url": btn["url"]
            }
            if "style" in btn and btn["style"]:
                button_config["style"] = btn["style"]
            elements.append(button_config)
        
        blocks.append({
            "type": "actions",
            "elements": elements
        })
    
    # Divider (if requested)
    if divider:
        blocks.append({"type": "divider"})
    
    return blocks

def log_failure(dag_id, start_date,end_date, failed_tasks, error_reasons, table='default.airflow_errors_tbl'):
    logging.error(f"CH LOGGING DISPARADO PARA A DAG: {dag_id}")
    try:
        from utils.get_clickhouse_client import get_clickhouse_client

        client = get_clickhouse_client('clickhouse_default')

        data = [[dag_id, start_date, end_date, failed_tasks[:10], error_reasons[:10]]]
        column_names = ['dag_id', 'start_date', 'end_date', 'failed_tasks', 'error_reasons']

        client.insert(table, data, column_names=column_names)
        
    except Exception as e:
        # Loga o erro de inserção mas não quebra o callback original
        logging.error(f"Erro ao inserir no ClickHouse: {e}")


def failure_callback_slack(
    context, 
    mention_users: Optional[List[str]] = None, 
    mention_prefix: str = "📢 Attention:",
    custom_message: Optional[str] = None,
    webhook_key: str = "slack_webhook",
    header_emoji: str = ":rotating_light:",
    header_text: str = "Execution Failure",
    additional_fields: Optional[Dict[str, str]] = None,
    button_text: str = "View Log",
    button_style: str = "danger",
    error_char_limit: int = 500,
    show_execution_date: bool = False,
    show_duration: bool = False
):
    """
    Extract Airflow context, build Block Kit message and call slack_alert.
    
    Args:
        context: Airflow context dictionary
        mention_users: List of Slack user IDs to mention
                      Accepted formats: ['U123456'], ['@username'], ['<@U123456>']
        mention_prefix: Prefix text before user mentions (default: "📢 Attention:")
        custom_message: Optional custom message before error details
        webhook_key: Airflow variable name with webhook URL (default: 'slack_webhook')
        header_emoji: Header emoji (default: ':rotating_light:')
        header_text: Header text (default: 'Execution Failure')
        additional_fields: Additional fields to display (dict with label: value)
        button_text: Log button text (default: 'View Log')
        button_style: Button style: 'danger', 'primary' or None (default: 'danger')
        error_char_limit: Maximum characters for error message (default: 500)
        show_execution_date: Show execution date in fields (default: False)
        show_duration: Show task duration in fields (default: False)
    """

    logging.error(f"CALLBACK DISPARADO PARA A DAG: {context.get('dag_run').dag_id}")
    # Safe data extraction
    dag_run = context.get('dag_run')
    task_instances = dag_run.get_task_instances(state='failed')

    # Build fields list
    fields = [
        {"type": "mrkdwn", "text": f"*DAG:*\n{dag_run.dag_id}"},
        {"type": "mrkdwn", "text": f"*RUN:*\n{dag_run.run_id}"}
    ]
    
    # Add execution date if requested
    if show_execution_date:
        fields.append({
            "type": "mrkdwn", 
            "text": f"*Execution Date:*\n{dag_run.execution_date}"
        })
    
    # Add extra fields (if provided)
    if additional_fields:
        for label, value in additional_fields.items():
            fields.append({"type": "mrkdwn", "text": f"*{label}:*\n{value}"})

    content_blocks = []

    failed_tasks_ids = []
    error_reasons = []

    primary_log_url = ""
    # Itera sobre cada task que falhou para compor o corpo da mensagem
    for ti in task_instances:
        if not primary_log_url:
            primary_log_url = ti.log_url
        
        failed_tasks_ids.append(ti.task_id)
        duration_str = f" ({ti.duration:.2f}s)" if show_duration and ti.duration else ""
        
        try:
            # 1. Tenta acessar o atributo 'error' (pode ser uma property ou campo)
            # Se for um 'bound method', chamamos com ()
            err = getattr(ti, 'error', None)
            
            if callable(err):
                error_msg = str(err())
            elif err:
                error_msg = str(err)
            # 2. Fallback: Se o contexto direto tiver a exceção da task atual
            elif context.get('exception'):
                error_msg = str(context.get('exception'))
        except Exception as e:
            error_msg = f"Error retrieving trace: {str(e)}"

        error_reasons.append(error_msg)
        if len(error_msg) > error_char_limit:
            error_msg = error_msg[:error_char_limit] + "... (truncated)"

        # Monta os blocos de cada erro
        content_blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"❌ *Task:* `{ti.task_id}`{duration_str}\n*Log:* <{ti.log_url}|View Log>"
            }
        })
        
        content_blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"```{error_msg}```"
            }
        })
        content_blocks.append({"type": "divider"})

    log_failure(
            dag_id=dag_run.dag_id,
            start_date=dag_run.start_date,
            end_date=datetime.now(),
            failed_tasks=failed_tasks_ids,   
            error_reasons=error_reasons      
        )
    
    blocks = build_slack_blocks(
        header_emoji=header_emoji,
        header_text=header_text,
        mention_users=mention_users,
        mention_prefix=mention_prefix,
        custom_message=custom_message,
        fields=fields,
        content_blocks=content_blocks, 
        buttons=[{
            "text": button_text,
            "url": primary_log_url,
            "style": button_style
        }]
    )

    payload = {
        "text": f"DAG Failure: {dag_run.dag_id}",
        "blocks": blocks
    }

    slack_alert(payload, webhook_key=webhook_key)


def success_callback_slack(
    context,
    mention_users: Optional[List[str]] = None,
    mention_prefix: str = "📢 FYI:",
    custom_message: Optional[str] = None,
    webhook_key: str = "slack_webhook",
    header_emoji: str = ":white_check_mark:",
    header_text: str = "Execution Success",
    additional_fields: Optional[Dict[str, str]] = None,
    button_text: str = "View Log",
    button_style: Optional[str] = "primary",
    show_execution_date: bool = False,
    show_duration: bool = True
):
    """
    Extract Airflow context, build Block Kit success message and call slack_alert.
    
    Args:
        context: Airflow context dictionary
        mention_users: List of Slack user IDs to mention
        mention_prefix: Prefix text before user mentions (default: "📢 FYI:")
        custom_message: Optional custom message
        webhook_key: Airflow variable name with webhook URL (default: 'slack_webhook')
        header_emoji: Header emoji (default: ':white_check_mark:')
        header_text: Header text (default: 'Execution Success')
        additional_fields: Additional fields to display (dict with label: value)
        button_text: Log button text (default: 'View Log')
        button_style: Button style: 'danger', 'primary' or None (default: 'primary')
        show_execution_date: Show execution date in fields (default: False)
        show_duration: Show task duration in fields (default: True)
    """
    # Safe data extraction
    dag_run = context.get('dag_run')
    task_instance = context.get('task_instance')
    log_url = task_instance.log_url
    
    # Build fields list
    fields = [
        {"type": "mrkdwn", "text": f"*DAG:*\n{dag_run.dag_id}"},
        {"type": "mrkdwn", "text": f"*Task:*\n{task_instance.task_id}"}
    ]
    
    # Add execution date if requested
    if show_execution_date:
        fields.append({
            "type": "mrkdwn", 
            "text": f"*Execution Date:*\n{dag_run.execution_date}"
        })
    
    # Add duration if requested
    if show_duration and task_instance.duration is not None:
        fields.append({
            "type": "mrkdwn", 
            "text": f"*Duration:*\n{task_instance.duration:.2f}s"
        })
    
    # Add extra fields (if provided)
    if additional_fields:
        for label, value in additional_fields.items():
            fields.append({"type": "mrkdwn", "text": f"*{label}:*\n{value}"})
    
    # Build blocks using the generic function
    blocks = build_slack_blocks(
        header_emoji=header_emoji,
        header_text=header_text,
        mention_users=mention_users,
        mention_prefix=mention_prefix,
        custom_message=custom_message,
        fields=fields,
        buttons=[{
            "text": button_text,
            "url": log_url,
            "style": button_style
        }]
    )

    # Build payload
    payload = {
        "text": f"DAG Success: {dag_run.dag_id}",
        "blocks": blocks
    }

    # Send to Slack
    slack_alert(payload, webhook_key=webhook_key)