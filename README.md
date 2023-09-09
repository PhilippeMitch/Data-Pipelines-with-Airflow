## Introduction

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They want to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Project Description

In this project, we created a custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step with Apache Airflow.

## Project Data
For this project, we have worked with two datasets. <br>
Here are the s3 links for each:
* Log data - 
```
s3://udacity-dend/log_data
```
* Song data -
```
s3://udacity-dend/song_data
```

## Project Folder
The package contains three major components for the project:
* The **dag template** has all the imports and task templates in place, but the task dependencies have not been set
* The **operators** folder with operator templates
* A **helper class** for the SQL transformations

### Project files
* `stage_redshift.py`: <br>
* `load_dimension.py`: <br>
* `load_fact.py`: <br>
* `data_quality.py`: <br>

## Project Environment
For this project we used:
* Python 3
* Apache Airflow
* Amazon Web Services (AWS)
* AWS Redshift
* AWS S3
### Prerequisites:
To be able to execute this project you will need to:
* Have an AWS Console account
* Create an IAM User in AWS.
* Configure Redshift Serverless in AWS.
### Setting up Connections
* Connect Airflow and AWS
* Connect Airflow to AWS Redshift Serverless
