import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import when, col
from pyspark.sql import SparkSession

# Get arguments
args = getResolvedOptions(sys.argv, ['s3_target_path_key', 's3_target_path_bucket'])
print(f"Received arguments: {args}")

fileName = args['s3_target_path_key']
bucket = args['s3_target_path_bucket']

# Initialize Spark session
spark = SparkSession.builder.appName("cdc").getOrCreate()

# Define file paths
inputFilePath = f"s3a://{bucket}/{fileName}"
finalFilePath = f"s3a://cdc-output-pyspark-hari/output"

# Check if fileName contains "LOAD"
if "LOAD" in fileName:
    # Load and rename columns
    fldf = spark.read.csv(inputFilePath, header=False)
    fldf = fldf.withColumnRenamed("_c0", "id") \
               .withColumnRenamed("_c1", "FullName") \
               .withColumnRenamed("_c2", "City")
    fldf.write.mode("overwrite").csv(finalFilePath, header=True)

else:
    # Load and rename columns
    udf = spark.read.csv(inputFilePath, header=False)
    udf = udf.withColumnRenamed("_c0", "action") \
             .withColumnRenamed("_c1", "id") \
             .withColumnRenamed("_c2", "FullName") \
             .withColumnRenamed("_c3", "City")
    
    # Read existing data
    fldf = spark.read.csv(finalFilePath, header=True)
    
    # Process actions
    for row in udf.collect():
        if row['action'] == 'U':
            fldf = fldf.withColumn("FullName", when(col('id') == row['id'], row['FullName']).otherwise(col('FullName')))
            fldf = fldf.withColumn("City", when(col('id') == row['id'], row['City']).otherwise(col('City')))
       
        elif row['action'] == 'I':
            insertedRow = [(row['id'], row['FullName'], row['City'])]
            columns = ['id', 'FullName', 'City']
            newdf = spark.createDataFrame(insertedRow, columns)
            fldf = fldf.union(newdf)
    
        elif row['action'] == 'D':
            fldf = fldf.filter(col('id') != row['id'])
    
    fldf.write.mode("overwrite").csv(finalFilePath, header=True)
