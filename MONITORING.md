# Monitoring & Alerting — BEES Brewery Pipeline

---

## What Is Monitored Today

The pipeline runs data quality checks at every layer and relies on Airflow's native task lifecycle for error propagation.

### Data Quality (Soda Core)

Soda Core evaluates checks on live PySpark DataFrames without collecting data to the driver. Check files live under `airflow/utils/soda_checks/`.

**Bronze — `bronze_checks.yml`**

| Check | Condition | Fail behaviour |
|---|---|---|
| Minimum record count | `row_count >= 100` | Marks `quality_check_bronze` as failed |
| No missing primary keys | `missing_count(id) = 0` | Same |
| No duplicate records | `duplicate_count(id) = 0` | Same |
| Required schema fields | `[id, name, brewery_type]` present | Same |

**Silver — `silver_checks.yml`**

| Check | Condition | Fail behaviour |
|---|---|---|
| No missing IDs | `missing_count(id) = 0` | Marks `quality_check_silver` as failed |
| No missing states | `missing_count(state) = 0` | Same |
| No missing countries | `missing_count(country) = 0` | Same |
| No duplicate records | `duplicate_count(id) = 0` | Same |
| Required schema fields | `[id, state, country]` present | Same |

**Gold — `gold_type_checks.yml`**

| Check | Condition | Fail behaviour |
|---|---|---|
| Minimum type rows | `row_count >= 3` | Marks `quality_check_gold` as failed |
| No missing brewery types | `missing_count(brewery_type) = 0` | Same |
| No duplicate types | `duplicate_count(brewery_type) = 0` | Same |
| Required schema fields | `[brewery_type, count]` present | Same |

**Gold — `gold_state_checks.yml`**

| Check | Condition | Fail behaviour |
|---|---|---|
| Minimum state rows | `row_count >= 10` | Marks `quality_check_gold` as failed |
| No missing states | `missing_count(state) = 0` | Same |
| No missing countries | `missing_count(country) = 0` | Same |
| Required schema fields | `[state, country, count]` present | Same |

Any failed Soda check raises `DataQualityError` inside the task. Airflow marks the task as `failed`, the DAG run stops, and the Dataset outlet is **not** published — so downstream DAGs (Silver, Gold) do not trigger.

### API Error Handling

`BreweryAPIClient` (in `airflow/utils/api_client.py`) uses a `requests.Session` with a `urllib3.Retry` adapter:

| Parameter | Value |
|---|---|
| `total` retries | `3` |
| `status_forcelist` | `429, 500, 502, 503, 504` |
| `allowed_methods` | `HEAD, GET, OPTIONS` |
| `backoff_factor` | `1` (exponential: 1 s, 2 s, 4 s) |
| Request `timeout` | `30 s` |
| Inter-page sleep | `0.1 s` |

Unrecoverable errors (`Timeout`, `HTTPError`, `ConnectionError`) are not caught in the client — they propagate to Airflow and mark the `ingest_from_api` task as `failed`.

### Logging

Airflow writes structured task logs accessible in the Airflow UI under each DAG run. Every task logs:
- `ingest_from_api`: pages fetched, records per page, total records
- `transform_to_parquet`: bronze count, silver count, output path
- `create_aggregations`: silver count, type-table rows, state-table rows
- `quality_check_*`: Soda scan result (total checks, passed, failed, warned)

### Dataset Lineage

The three DAGs are wired as an Airflow Dataset chain:

```
bees_ingestion_api_bronze_brewery_data  →  publishes bronze_dataset
bees_ingestion_api_silver_brewery_data  →  consumes bronze_dataset, publishes silver_dataset
bees_ingestion_api_gold_brewery_data    →  consumes silver_dataset
```

Airflow tracks which DAG run produced each dataset version, making it possible to trace a Gold table back to the exact Bronze ingestion that sourced it.

---

## Production Monitoring Strategy

In a production environment, the recommended stack is **Soda Core + CloudWatch + Grafana + Slack**.

### 1. Metrics Collection — DAGs → CloudWatch

Each task publishes custom metrics after execution:

```python
import boto3

cloudwatch = boto3.client('cloudwatch')
cloudwatch.put_metric_data(
    Namespace='BreweryPipeline',
    MetricData=[
        {
            'MetricName': 'RecordsProcessed',
            'Value': record_count,
            'Dimensions': [{'Name': 'Layer', 'Value': 'Bronze'}]
        },
        {
            'MetricName': 'TransformationDuration',
            'Value': duration_seconds,
            'Dimensions': [{'Name': 'Layer', 'Value': 'Silver'}]
        },
        {
            'MetricName': 'QualityChecksPassed',
            'Value': 1 if all_passed else 0,
            'Dimensions': [{'Name': 'Layer', 'Value': 'Gold'}]
        }
    ]
)
```

**Infrastructure metrics collected automatically by CloudWatch:**
- Container: CPU %, memory %, disk I/O
- RDS (Airflow metadata DB): connections, query latency
- ECS: active tasks, task failure count

**Log routing:** Airflow task logs stream to CloudWatch Logs as structured JSON for querying with CloudWatch Logs Insights.

### 2. Alerting

**Path A — Task failures: Airflow → Slack**

```python
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

def task_failure_alert(context):
    SlackWebhookOperator(
        task_id='slack_alert',
        slack_webhook_conn_id='slack_webhook',
        message=(
            ":red_circle: *Task Failed*\n"
            f"*DAG*: {context['task_instance'].dag_id}\n"
            f"*Task*: {context['task_instance'].task_id}\n"
            f"*Log*: {context['task_instance'].log_url}"
        )
    ).execute(context=context)

default_args = {'on_failure_callback': task_failure_alert}
```

**Path B — Metric thresholds: CloudWatch → SNS → Slack**

```
CloudWatch Alarm (e.g. RecordsProcessed < 5000)
    → SNS Topic
    → Slack webhook
    → Alert in #data-engineering channel
```

**Recommended CloudWatch Alarms:**

| Metric | Condition | Alarm name | Severity |
|---|---|---|---|
| `RecordsProcessed` (Bronze) | `< 5000` | `LowBronzeVolume` | Warning |
| `QualityChecksPassed` | `= 0` | `QualityCheckFailed` | Critical |
| `TransformationDuration` (Silver) | `> 1500 s` | `SlowSilverTransform` | Warning |
| `DataAge` | `> 26 h` | `StaleGoldData` | Critical |
| `MemoryUtilization` | `> 80%` for 10 min | `HighMemory` | Warning |

### 3. Visualization — Grafana

Grafana dashboards connect to CloudWatch as a data source.

**Dashboard 1: Pipeline Health**
- Records processed per layer per day (time series)
- DAG run success rate over 7 days (stat panel)
- Task duration trends by layer (bar chart)

**Dashboard 2: Data Quality**
- Soda check pass/fail rate by layer (gauge)
- Null percentage in `id`, `state`, `country` fields over time (time series)
- Row count anomalies vs. 7-day rolling average (alert panel)

**Dashboard 3: Infrastructure**
- Container CPU and memory utilisation (time series)
- Disk usage trend with threshold line (stat panel)
- Active Airflow workers and scheduler heartbeat (single stat)

### 4. Incident Response

When a Slack alert fires:

1. Open the Airflow UI link included in the alert — inspect the task log.
2. Check the Grafana Pipeline Health dashboard for context (was this a spike or a trend?).
3. Query CloudWatch Logs Insights for structured error details.
4. If it is an infrastructure issue, check CloudWatch Container Insights.
5. Re-trigger the Bronze DAG after the fix; Silver and Gold re-run automatically via Dataset triggers.

---

## Key Tools

| Tool | Role |
|---|---|
| **Soda Core Spark 3.3.2** | In-process data quality checks on Spark DataFrames, YAML-defined contracts per layer |
| **Apache Airflow 2.8.0** | DAG scheduling, task-level failure callbacks, Dataset lineage |
| **CloudWatch** | Custom metric storage, alarms, log aggregation |
| **Grafana** | Real-time dashboards and threshold visualisation |
| **Slack** | Alert delivery to engineering team |
