"""
Airflow DAG callbacks for alerting on failures and retries.

Sends email notifications to the configured alert email address
when tasks fail or are retried.
"""

import logging
from datetime import datetime

from airflow.utils.email import send_email

logger = logging.getLogger(__name__)

ALERT_EMAIL = "wira.hutomo2@gmail.com"


def on_failure_callback(context: dict) -> None:
    """
    Callback triggered when a task fails.

    Sends a detailed email with DAG info, task info, error message,
    and a link to the Airflow logs.
    """
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    task_id = task_instance.task_id
    execution_date = context.get("execution_date")
    exception = context.get("exception")
    log_url = task_instance.log_url
    try_number = task_instance.try_number

    subject = f"🔴 PIPELINE FAILURE: {dag_id}.{task_id}"

    html_content = f"""
    <div style="font-family: Arial, sans-serif; max-width: 600px;">
        <h2 style="color: #d32f2f;">🔴 Pipeline Task Failure</h2>
        
        <table style="width: 100%; border-collapse: collapse; margin: 16px 0;">
            <tr style="background: #f5f5f5;">
                <td style="padding: 8px; font-weight: bold; border: 1px solid #ddd;">DAG</td>
                <td style="padding: 8px; border: 1px solid #ddd;">{dag_id}</td>
            </tr>
            <tr>
                <td style="padding: 8px; font-weight: bold; border: 1px solid #ddd;">Task</td>
                <td style="padding: 8px; border: 1px solid #ddd;">{task_id}</td>
            </tr>
            <tr style="background: #f5f5f5;">
                <td style="padding: 8px; font-weight: bold; border: 1px solid #ddd;">Execution Date</td>
                <td style="padding: 8px; border: 1px solid #ddd;">{execution_date}</td>
            </tr>
            <tr>
                <td style="padding: 8px; font-weight: bold; border: 1px solid #ddd;">Try Number</td>
                <td style="padding: 8px; border: 1px solid #ddd;">{try_number}</td>
            </tr>
            <tr style="background: #f5f5f5;">
                <td style="padding: 8px; font-weight: bold; border: 1px solid #ddd;">Timestamp</td>
                <td style="padding: 8px; border: 1px solid #ddd;">{datetime.utcnow().isoformat()}Z</td>
            </tr>
        </table>

        <h3 style="color: #d32f2f;">Error Details</h3>
        <pre style="background: #fff3f3; padding: 12px; border-radius: 4px; 
                    border-left: 4px solid #d32f2f; overflow-x: auto;">
{str(exception)[:2000]}
        </pre>

        <p>
            <a href="{log_url}" style="display: inline-block; padding: 10px 20px;
               background: #1976d2; color: white; text-decoration: none; 
               border-radius: 4px;">
                📋 View Task Logs
            </a>
        </p>

        <hr style="border: none; border-top: 1px solid #ddd; margin: 24px 0;">
        <p style="color: #666; font-size: 12px;">
            Data Lakehouse ELT Pipeline — GCP + BigLake + Airflow (Astro)
        </p>
    </div>
    """

    try:
        send_email(
            to=ALERT_EMAIL,
            subject=subject,
            html_content=html_content,
        )
        logger.info(f"Failure alert sent for {dag_id}.{task_id}")
    except Exception as e:
        logger.error(f"Failed to send failure alert email: {e}")
        # Log to Cloud Logging as fallback
        logger.critical(
            f"PIPELINE_FAILURE|dag={dag_id}|task={task_id}|"
            f"execution_date={execution_date}|error={str(exception)[:500]}"
        )


def on_retry_callback(context: dict) -> None:
    """
    Callback triggered when a task is retried.
    Logs retry information for monitoring.
    """
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    task_id = task_instance.task_id
    try_number = task_instance.try_number
    exception = context.get("exception")

    logger.warning(
        f"PIPELINE_RETRY|dag={dag_id}|task={task_id}|"
        f"try_number={try_number}|error={str(exception)[:500]}"
    )


def on_success_callback(context: dict) -> None:
    """
    Callback triggered when a DAG run completes successfully.
    Optional: can be used for success notifications.
    """
    dag_id = context.get("dag").dag_id
    execution_date = context.get("execution_date")

    logger.info(
        f"PIPELINE_SUCCESS|dag={dag_id}|execution_date={execution_date}"
    )
