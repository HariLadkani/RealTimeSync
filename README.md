# RealTimeSync

RealTimeSync is an advanced Change Data Capture (CDC) pipeline designed to efficiently manage and process real-time data changes from an Amazon RDS database. This project integrates multiple AWS services to ensure accurate and timely data synchronization across systems.

Overview:

RealTimeSync captures changes from a source RDS database, processes them through AWS DMS, and manages the data flow through AWS S3, Lambda, and Glue. It leverages a serverless architecture and scalable components to provide a robust and efficient solution for real-time data integration.

Architecture

![image](https://github.com/user-attachments/assets/b71cd460-ef65-4937-a07d-9da5c1206d90)



Amazon RDS: Source database where changes are monitored and captured.


AWS Database Migration Service (DMS): Migrates changes from the RDS database to a temporary Amazon S3 bucket.


Amazon S3: Temporary storage for captured data changes. S3 events trigger downstream processing.


AWS Lambda: Serverless function that responds to S3 events, triggering the ETL process.


AWS Glue with PySpark: Performs data transformation and synchronization, updating the final S3 bucket with the processed changes.


Features


Real-Time Data Capture: Monitors and captures database changes in real-time.


Serverless Processing: Uses AWS Lambda to handle event-driven data processing.


Scalable Data Transformation: Utilizes AWS Glue and PySpark for scalable and efficient data transformations.


Flexible Integration: Easily integrates with various AWS services for a comprehensive data management solution.

