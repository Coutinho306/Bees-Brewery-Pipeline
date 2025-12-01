## Monitoring & Alerting

### Current Implementation
- **Data Quality**: Great Expectations validation at each layer (Bronze/Silver/Gold)
- **Error Handling**: Automatic retries with exponential backoff on API failures
- **Logging**: Comprehensive logging via Airflow UI for all task executions
- **Dataset Lineage**: Dataset-driven orchestration for automatic dependency tracking

### Production Monitoring Strategy

For production, I would implement monitoring using **CloudWatch + Grafana + Slack**. Here's the complete flow:

#### 1. Data Collection & Metrics

**From DAG Tasks → CloudWatch:**
```python
import boto3

# Publish custom metrics from each DAG task
cloudwatch = boto3.client('cloudwatch')
cloudwatch.put_metric_data(
    Namespace='BreweryPipeline',
    MetricData=[
        {'MetricName': 'RecordsProcessed', 'Value': count, 'Dimensions': [{'Name': 'Layer', 'Value': 'Bronze'}]},
        {'MetricName': 'TransformationDuration', 'Value': duration_seconds, 'Dimensions': [{'Name': 'Layer', 'Value': 'Silver'}]},
        {'MetricName': 'QualityChecksPassed', 'Value': 1 if passed else 0, 'Dimensions': [{'Name': 'Layer', 'Value': 'Gold'}]}
    ]
)
```

**Infrastructure Metrics (Auto-collected by CloudWatch):**
- Container: CPU, Memory, Disk utilization
- RDS: Database connections, query performance
- ECS: Active tasks, task failures

**Logs → CloudWatch Logs:**
- Airflow task logs automatically streamed to CloudWatch Logs
- Structured JSON logging for easy querying via CloudWatch Logs Insights

#### 2. Alerting Pipeline

**Two Alert Paths:**

**Path A - Task Failures (Airflow → Slack):**
```python
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

def task_failure_alert(context):
    """Immediate Slack alert on task failure."""
    alert = SlackWebhookOperator(
        task_id='slack_alert',
        slack_webhook_conn_id='slack_webhook',
        message=f":red_circle: *Task Failed*\n*DAG*: {context['task_instance'].dag_id}\n*Log*: {context['task_instance'].log_url}"
    )
    return alert.execute(context=context)

# Apply to all DAGs
default_args = {'on_failure_callback': task_failure_alert}
```

**Path B - Metric Thresholds (CloudWatch → SNS → Slack):**
```
CloudWatch Metric (e.g., RecordsProcessed < 5000)
    ↓
CloudWatch Alarm triggered
    ↓
SNS Topic notification
    ↓
Slack webhook integration
    ↓
Alert in #data-engineering channel
```

**Example CloudWatch Alarms:**
- `RecordsProcessed` < 5000 for Bronze layer → Low volume alert
- `MemoryUtilization` > 80% for 10 minutes → Resource alert
- `DataAge` > 26 hours → Stale data alert
- `QualityChecksPassed` = 0 → Data quality alert

#### 3. Visualization (Grafana)

**Grafana dashboards pulling from CloudWatch data sources:**

**Dashboard 1: Pipeline Health**
- Records processed by layer (time series chart)
- DAG run success rate (stat panel)
- Task duration trends (bar chart)

**Dashboard 2: Data Quality**
- Quality check pass/fail rate (gauge)
- Null percentage in critical fields (time series)
- Record count anomalies (alert panel)

**Dashboard 3: Infrastructure**
- CPU/Memory utilization (time series)
- Disk usage trends (stat panel with threshold)
- Active Airflow workers (single stat)

#### 4. Incident Response

**When Alert Fires:**
1. Slack alert received with Airflow log URL
2. Check Grafana dashboard for trend context
3. Query CloudWatch Logs Insights for error details:
4. Infrastructure issue: Check CloudWatch Container Insights
5. Fix and monitor via Grafana for resolution

## Key Tools Summary

- **CloudWatch**: Metrics storage, alarms, log aggregation
- **Grafana**: Real-time dashboards and visualization
- **Slack**: Alert delivery to engineering team
- **Great Expectations**: In-code data quality validation
