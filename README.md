# Music Streaming Pipeline

Creating a high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills including data quality plays on top the data warehouse and running tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.
