# Project: STEDI Human Balance Analytics

in this project we will use AWS Glue, AWS S3, Python, and Spark, create or generate Python scripts to build a lakehouse solution in AWS that satisfies these requirements from the STEDI data scientists. we will need to you'll need to extract the data produced by the STEDI Step Trainer sensors and the mobile app, and curate them into a data lakehouse solution on AWS so that Data Scientists can train the learning model

## prerequisite

1. AWS account
2. s3 bucket

## steps
1. copy the source data to the S3 bucket and place it in the relevant folder
   1. there were some problems with the source data so we need to prepare the data before loading it to S3
      1. the data is store as one line json and when athena read it from the json file, it is only reading the first record
      2. to solve this i wrote a small python file to split the json so each record will be in saperated line
2. run the customers_trusted job in glue to Sanitize the Customer data
   1. the source data have some problem so the saniteze data have a few steps
      1. read data from the customers_landing folder
      2. remove duplicates - the source data have some duplication , nad semi duplication (same user details with multiple birthdays or serialnumbers)
      3. calculate age based on the birthday fields (the sample data contain some with wrong data in the birthday field)
      4. remove users age are more then 120 (can be configured in the job)
      5. filter users that did not agree to share there inforamtion
3. run the accelerotor_trusted glue job to Sanitize the Accelerometer data from the Mobile App
4. create the customers_trusted and the accelerometer_trusted tables
5. run the customers_curated to Sanitize the Customer data (Trusted Zone) and create a Glue Table (Curated Zone) that only includes customers who have accelerometer data and have agreed to share their data for research
6. load the step_trainer data to step_trainer_landiing
7. run step_trainer_trusted job to crate table that contains the Step Trainer Records data for customers who have accelerometer data and have agreed to share their data for research
8. run the machine_learning_curated job to crate table for analytucs that aggregated table that has each of the Step Trainer Readings, and the associated accelerometer reading data for the same timestamp, but only for customers who have agreed to share their data

## folders in this repository
- python    - contains the python scripts of the glue jobs and the split_json script
- screenshots  - contains screenshots of each glue job , screenshots of each table preview query, and screenshot of each count query
- sql       - the sql query for creating each of the tables


please see my question about the data problems 
https://knowledge.udacity.com/questions/953339

