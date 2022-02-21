# Udacity-project-Data-pipeline

## Project Introduction 

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to  create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

## Business Process
1. Extract the source data from S3 and copy to redshift
The source log and song data, which is assumed to be produced from the apps, are saved in s3://udacity-dend/log_data and s3://udacity-dend/song_data.  
The first step is to extract all log and song data from S3 to Redshift as a staging table

2. Normalize the data 
The second step is to create fact and dimension tables, and insert the relevant data from staging table to those fact and dimension tables 

3. Conduct the quality check. <br />
After inserting the data to the fact and dimension tables, the next step is to check if the data is successfully inserted

Steps afterwards: 
With the inserted data, BI analyst and data scientist can create the dashboards or predictive models. <br />
However, this part will be out of the scope of this project. 

## Major components in the pipeline  
There are 3 major components in this pipeline 

**DAG** - The orchestrator of the pipeline. It provides the sequences of the tasks that nees to be run, and other detail such as time of retries if failed.<br />
<br />
**Helper** - The SQL scripts that needs to be run in each subtask. The sql_queries.py under the helper folder contains the create_table and insert_table scripts for all the tables that are needed in the pipeline. <br />
<br />
**Operators** -- The task. In this pipeline, the tasks are in the similar structure as the business process <br />
  - **StageToRedshiftOperator** -- Extract the source data from S3 and copy to staging table in redshift
  - **LoadFactOperator** -- Load the fact data from the staging table to fact table 
  - **LoadDimensionOperator** -- Load the dimension data from the staging table to fact table 
  - **DataQualityOperator** -- Check if the table consists data 

## DAG
In the DAG, add default parameters according to these guidelines

The DAG does not have dependencies on past runs
- On failure, the task are retried 3 times
- Retries happen every 5 minutes
- Catchup is turned off
- Do not email on retry

The DAG running sequence should be as follows
![image](https://user-images.githubusercontent.com/38469208/154936539-bfea9e66-75f7-47b6-a441-43871d445a3a.png)
