import os
import json
import urllib3
import boto3


def lambda_handler(event, context):
    host = os.environ['DATABRICKS_HOST']
    token = os.environ['DATABRICKS_TOKEN']
    sns_arn = os.environ['SNS_TOPIC_ARN']
    warehouse_id = os.environ['WAREHOUSE_ID']

    http = urllib3.PoolManager()
    url = f"{host}/api/2.0/sql/statements"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    query = f"""
        SELECT
        timestamp,
        error.exceptions[0].message AS error_detail,
        regexp_extract(error.exceptions[0].message, "Violated expectations: '([^']+)'", 1) AS violated_rule,
        regexp_extract(error.exceptions[0].message, "Input data: '(.+)'", 1) AS offending_record,
        origin.pipeline_id
        FROM event_log("ab370c25-0a6c-4350-8db5-3a5f1d8eac26")
        WHERE level = 'ERROR'
        -- AND timestamp >= (current_timestamp() - INTERVAL 20 MINUTES)
        AND size(error.exceptions) > 0
        AND error.exceptions[0].message IS NOT NULL
        AND error.exceptions[0].message LIKE '%failed to meet the expectation%'
        ORDER BY timestamp DESC
    """

    body = json.dumps({"warehouse_id": warehouse_id, "statement": query})

    response = http.request("POST", url, headers=headers, body=body)
    data = json.loads(response.data.decode('utf-8'))
    print("Response status and data:", response.status)
    print(data)

    if data.get('result', {}).get('row_count', 0) > 0:
        sns = boto3.client('sns')
        # sns.publish(
        #     TopicArn=sns_arn,
        #     Subject="Databricks Pipeline Error Alert",
        #     Message=f"Errors found in pipeline: {json.dumps(data['result']['data_array'], indent=2)}"
        # )
        return "Alert Sent"

    return "No Errors Found"
