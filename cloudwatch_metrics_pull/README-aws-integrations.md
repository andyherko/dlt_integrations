# The Architectural Paradigm of Spark Declarative Pipelines

### Purpose: 
Integrate Databricks Lakeflow Declarative Pipeline processing events stored in databricks with AWS CloudWatch for improved telemetry and alerting.

## Integration 1 - Pull Architecture
Pulling with AWS Lambda by quering databricks sql warehouse event logs with status type 'ERROR' using event_log() sql function. 
AWS SNS will receive error events sent by Lambda and send email alerts to users.

### **Demo Check List**
#### AWS
- EventBridge schedule 'Databricks_DLT_Pipeline_Health_Check' enabled
- Lambda contains interval condition in sql statement and version is deployed. (AND timestamp >= (current_timestamp() - INTERVAL 20 MINUTES))

#### Databricks
- SQL Warehouse running
- 
