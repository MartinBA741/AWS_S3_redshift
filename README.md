# AWS_S3_redshift
This project creates a data warehouse hosted on AWS, running an ETL pipeline for a database hosted on Redshift. The ETL pipeline extracts data from AWS S3, stages them in Redshift, and transforms data into a set of dimensional tables analytical purposes. 

## Ropository contains
1) sql_queries.py is a collection of sql queries. It includes statements on creating, staging and dropping tables.
2) create_table.py is a python script connecting to redshift and call the sql queries from sql_queries.py
3) etl.py is an etl pipeline connecting to redshift and call the sql queries from sql_queries.py 

Note that the files are refering to a private dwh.cfg (not on github), which direct them to the AWS S3 data and IAM role.
