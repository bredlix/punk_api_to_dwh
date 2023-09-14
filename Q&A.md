### Q&A

1. the only concern regarding current implementation is the usage of Python on airflow worker for parsing
jsonl files. this task might become a bottleneck in PROD case since amount of logs to parse will overload 
Airflow and block resources for other pipelines. I would use any other tools that can process jsonl (postgres cannot) like bigquery, spark. 
in general Airflow should not ever be use for processing of anything. it's eventually and orchestrator tool.
2. it seems like there is some corr, but to give a clear answer i would need more time for investigation
3. missing data, incorrect data and any other possible issue. it's an external data source, moreover API.
devs could change code, approach or simply add AUTH to break current solution.
data quality tests, alerting on failure ect. it always depends on the purpose and the nature of data.
4. start raw data append and simply do slowly changing dimensions. i'd implement SCD type 2