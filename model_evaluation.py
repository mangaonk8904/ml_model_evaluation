# dags/trust_business_eval_dag.py
from __future__ import annotations
from datetime import datetime, timedelta
from typing import Dict, Any, List
import os
import json

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

# --- Spark / Data stack ---
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, expr, when, current_timestamp

# --- Local modules you added earlier ---
from trust_eval.config_schema import load_config, Config  # (from previous message)

# ========= Helpers =========

def get_spark() -> SparkSession:
    spark = SparkSession.builder.appName("trust-business-eval").enableHiveSupport().getOrCreate()
    # Nice-to-haves; tweak per env
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark

def run_sql(spark: SparkSession, sql: str) -> DataFrame:
    return spark.sql(sql)

# ========= Metric Runners =========

def incident_reduction_runner(spark: SparkSession, cfg: Config, ds: str, mconf: Dict[str, Any]) -> DataFrame:
    """
    Compare incident counts between current cohort impacted by model vs baseline.
    Produces rows: model_name, model_version, metric_id, metric_value, numerator, denominator, ds
    """
    incident_table = mconf["params"]["incident_table"]
    window_days = int(mconf["params"].get("window_days", 7))
    incident_filter = mconf["params"].get("incident_filter", "1=1")

    # Current window
    cur_sql = f"""
      WITH cur AS (
        SELECT *
        FROM {incident_table}
        WHERE ds BETWEEN date_sub('{ds}', {window_days-1}) AND '{ds}'
          AND {incident_filter}
      )
      SELECT COUNT(1) AS cur_incidents FROM cur
    """
    cur = run_sql(spark, cur_sql).collect()[0]["cur_incidents"]

    # Baseline — simplest prev window (you can switch to version-based via cfg if needed)
    lookback_days = int(mconf["params"].get("baseline", {}).get("lookback_days", window_days))
    base_sql = f"""
      WITH base AS (
        SELECT *
        FROM {incident_table}
        WHERE ds BETWEEN date_sub('{ds}', {window_days + lookback_days - 1}) AND date_sub('{ds}', {lookback_days})
          AND {incident_filter}
      )
      SELECT COUNT(1) AS base_incidents FROM base
    """
    base = run_sql(spark, base_sql).collect()[0]["base_incidents"]

    # Reduction = (base - cur) / base
    reduction = 0.0 if base == 0 else (base - cur) / float(base)
    # Return a tiny DF
    out = spark.createDataFrame(
        [(cfg.model.name, "auto", mconf["id"], float(reduction), int(cur), int(base), ds)],
        ["model_name", "model_version", "metric_id", "metric_value", "numerator", "denominator", "ds"]
    )
    return out

def ratio_runner(spark: SparkSession, cfg: Config, ds: str, mconf: Dict[str, Any], source_sql: str) -> DataFrame:
    """
    Ratio = numerator_sql / denominator_sql over a provided FROM clause.
    `source_sql` should materialize a table/view named tmp_source for this ds.
    """
    params = mconf["params"]
    numerator_sql = params["numerator_sql"]
    denominator_sql = params["denominator_sql"]

    run_sql(spark, f"DROP VIEW IF EXISTS tmp_source")
    run_sql(spark, f"CREATE TEMP VIEW tmp_source AS {source_sql}")

    q = f"""
      SELECT
        {numerator_sql} AS numerator,
        {denominator_sql} AS denominator
      FROM tmp_source
    """
    row = run_sql(spark, q).collect()[0]
    num, den = int(row["numerator"]), int(row["denominator"])
    value = 0.0 if den == 0 else num / float(den)
    return spark.createDataFrame(
        [(cfg.model.name, "auto", mconf["id"], float(value), num, den, ds)],
        ["model_name", "model_version", "metric_id", "metric_value", "numerator", "denominator", "ds"]
    )

def delta_runner(spark: SparkSession, cfg: Config, ds: str, mconf: Dict[str, Any]) -> DataFrame:
    """
    Compare a count with a reference (e.g., prev_7d_avg).
    """
    st = mconf["params"]["source_table"]
    count_expr = mconf["params"]["count_expr"]
    compare_to = mconf["params"].get("compare_to", "prev_7d_avg")

    cur_sql = f"SELECT {count_expr} AS cur FROM {st} WHERE ds = '{ds}'"
    cur = run_sql(spark, cur_sql).collect()[0]["cur"]

    if compare_to == "prev_7d_avg":
        ref_sql = f"""
          SELECT AVG(cnt) AS ref
          FROM (
            SELECT {count_expr} AS cnt
            FROM {st}
            WHERE ds BETWEEN date_sub('{ds}', 7) AND date_sub('{ds}', 1)
          )
        """
    else:
        # fallback: previous day
        ref_sql = f"SELECT {count_expr} AS ref FROM {st} WHERE ds = date_sub('{ds}', 1)"
    ref_row = run_sql(spark, ref_sql).collect()[0]
    ref = float(ref_row["ref"]) if ref_row["ref"] is not None else 0.0
    delta = 0.0 if ref == 0 else (cur - ref) / ref
    return spark.createDataFrame(
        [(cfg.model.name, "auto", mconf["id"], float(delta), int(cur), float(ref), ds)],
        ["model_name", "model_version", "metric_id", "metric_value", "numerator", "denominator", "ds"]
    )

# ========= DAG =========

@dag(
    schedule=None,  # or "0 6 * * *"
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        "owner": "trust-ml",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["trust-eval", "business-metrics"],
)
def trust_business_eval():
    """
    Generic business-metric evaluator for Trust ML models. 
    Onboards models by YAML config.
    """

    @task
    def load_yaml_config(config_path: str) -> Dict[str, Any]:
        cfg = load_config(config_path)
        return json.loads(cfg.json())

    @task
    def quality_checks(cfg_json: Dict[str, Any], ds: str) -> Dict[str, Any]:
        # Minimal guardrail: make sure required partitions exist
        spark = get_spark()
        cfg = Config.parse_obj(cfg_json)

        def must_have(table: str, date_col: str):
            cnt = run_sql(
                spark, f"SELECT COUNT(*) AS c FROM {table} WHERE {date_col} = '{ds}'"
            ).collect()[0]["c"]
            if cnt == 0:
                raise ValueError(f"Missing partition for {table} at {date_col}={ds}")

        must_have(cfg.inputs.predictions.table, cfg.inputs.predictions.date_column)
        must_have(cfg.inputs.labels.table, cfg.inputs.labels.date_column)
        return cfg_json

    @task
    def evaluate_metrics(cfg_json: Dict[str, Any], ds: str) -> str:
        spark = get_spark()
        cfg = Config.parse_obj(cfg_json)
        all_out: List[DataFrame] = []

        # Example: a “source view” often used by ratios (you’ll tailor per model)
        # Make a light, model-scoped daily view of predictions joined to suspensions if needed.
        base_view_sql = f"""
          SELECT *
          FROM {cfg.inputs.predictions.table}
          WHERE {cfg.inputs.predictions.date_column} = '{ds}'
            AND ({cfg.inputs.predictions.where or '1=1'})
        """

        for m in cfg.metrics:
            if m.runner == "incident_reduction":
                out = incident_reduction_runner(spark, cfg, ds, m.dict())
                all_out.append(out)
            elif m.runner == "ratio":
                out = ratio_runner(spark, cfg, ds, m.dict(), source_sql=base_view_sql)
                all_out.append(out)
            elif m.runner == "delta":
                out = delta_runner(spark, cfg, ds, m.dict())
                all_out.append(out)
            elif m.runner == "custom_sql":
                # You can extend: execute SQL & reduce to (value, num, den)
                raise NotImplementedError("custom_sql runner not implemented here.")
            else:
                raise ValueError(f"Unknown runner: {m.runner}")

        if not all_out:
            return "[]"

        final_df = all_out[0]
        for df in all_out[1:]:
            final_df = final_df.unionByName(df, allowMissingColumns=True)

        # Write to metrics table
        final_df = final_df.withColumn("ts_computed_at", current_timestamp()) \
                           .withColumn("config_hash", lit(cfg.hash()))
        final_df.createOrReplaceTempView("tmp_metrics")
        run_sql(
            spark,
            f"""
            INSERT INTO {cfg.outputs.metrics_table}
            SELECT * FROM tmp_metrics
            """
        )
        # Also write to audit with one row per metric_id
        audit_df = final_df.selectExpr(
            "model_name","model_version","metric_id","ds",
            "config_hash","ts_computed_at",
            "CAST(NULL AS STRING) AS status","CAST(NULL AS STRING) AS error_msg"
        )
        audit_df.createOrReplaceTempView("tmp_audit")
        run_sql(spark, f"INSERT INTO {cfg.outputs.audit_table} SELECT * FROM tmp_audit")
        return "ok"

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def notify(cfg_json: Dict[str, Any], eval_status: str) -> None:
        cfg = Config.parse_obj(cfg_json)
        # Hook to Slack/Email; for now, print
        print(f"[trust-eval] model={cfg.model.name} status={eval_status}")

    # --- Wire it ---
    # Example: read from Airflow Variable or pass a default
    config_path = Variable.get("trust_eval_config_path", default_var="configs/models/connected_accounts_redaction.yaml")
    ds = "{{ ds }}"

    cfg_json = load_yaml_config(config_path)
    checked_cfg = quality_checks(cfg_json, ds)
    status = evaluate_metrics(checked_cfg, ds)
    notify(checked_cfg, status)

dag = trust_business_eval()
