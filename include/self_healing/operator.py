"""
SelfHealingOperator: A deferrable Airflow Operator that runs the OpenAI healing loop,
then uses the Triggerer to sleep during the cooldown period (no wasted worker slots).
"""
import os
import logging
from datetime import timedelta

from airflow.models import BaseOperator, Variable
from airflow.triggers.temporal import TimeDeltaTrigger

logger = logging.getLogger("self_healing_operator")


class SelfHealingOperator(BaseOperator):
    """
    Runs the OpenAI-powered self-healing agent loop once, then defers
    for `cooldown_seconds` before the @continuous schedule re-triggers it.
    """

    def __init__(
        self,
        *,
        cooldown_seconds: int = 90,
        model: str = "gpt-4o",  # Default: gpt-4o. Change to "gpt-5" when available.
        target_dag_id: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.cooldown_seconds = cooldown_seconds
        self.model = model
        self.target_dag_id = target_dag_id

    def execute(self, context):
        """Run the OpenAI healing loop, then defer for the cooldown period."""
        self._ensure_api_key()

        # Allow runtime override via Airflow Variables (set in Admin > Variables)
        target = self.target_dag_id or Variable.get(
            "self_healing_target_dag", default_var=None
        )
        model = Variable.get("self_healing_model", default_var=self.model)

        logger.info(f"🤖 Self-Healing Agent starting. Model: {model}, Target: {target or 'ALL DAGs'}")

        # Import here to avoid import errors if openai is not installed
        from include.self_healing.agent import run_healing_loop

        report = run_healing_loop(dag_id=target, model=model)

        logger.info("=" * 60)
        logger.info("🏥 SELF-HEALING AGENT REPORT")
        logger.info("=" * 60)
        logger.info(report)
        logger.info("=" * 60)

        # Push the report to XCom for audit trail
        context["ti"].xcom_push(key="healing_report", value=report)
        context["ti"].xcom_push(key="target_dag", value=target or "all")

        logger.info(f"⏳ Cooling down for {self.cooldown_seconds}s before next cycle...")

        # Defer — release the worker slot during the cooldown period
        self.defer(
            trigger=TimeDeltaTrigger(timedelta(seconds=self.cooldown_seconds)),
            method_name="execute_complete",
        )

    def execute_complete(self, context, event=None):
        """Called once the cooldown timer fires. The @continuous schedule restarts the DAG."""
        logger.info(f"✅ Cooldown of {self.cooldown_seconds}s complete. DAG will re-trigger via @continuous schedule.")

    def _ensure_api_key(self):
        """Load OpenAI + Airflow credentials from Variables if not in the environment."""
        # --- OpenAI API Key ---
        if not os.getenv("OPENAI_API_KEY"):
            api_key = Variable.get("OPENAI_API_KEY", default_var=None)
            if api_key:
                os.environ["OPENAI_API_KEY"] = api_key
                logger.info("✅ OPENAI_API_KEY loaded from Airflow Variables.")
            else:
                raise ValueError(
                    "OPENAI_API_KEY not found. Set it as an environment variable or "
                    "an Airflow Variable in Admin > Variables."
                )

        # --- Airflow REST API credentials ---
        # Try to load an API token first (preferred for Airflow 3)
        if not os.getenv("AIRFLOW_API_TOKEN"):
            token = Variable.get("AIRFLOW_API_TOKEN", default_var=None)
            if token:
                os.environ["AIRFLOW_API_TOKEN"] = token
                logger.info("✅ AIRFLOW_API_TOKEN loaded from Airflow Variables.")

        # Fall back to basic auth credentials
        if not os.getenv("AIRFLOW_USERNAME"):
            username = Variable.get("AIRFLOW_USERNAME", default_var="admin")
            os.environ["AIRFLOW_USERNAME"] = username

        if not os.getenv("AIRFLOW_PASSWORD"):
            password = Variable.get("AIRFLOW_PASSWORD", default_var="admin")
            os.environ["AIRFLOW_PASSWORD"] = password
