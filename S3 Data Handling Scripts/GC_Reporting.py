#Dependencies
##!pip install boto3

#Code :
# Importing Libraries
import sys
import boto3
import datetime
import pandas as pd
import os

# import findspark
# findspark.init()
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.master("local[*]").getOrCreate()


# dataset = spark.read.csv('user_credentials.csv',inferSchema=True, header =True)
# df = pd.read_parquet('GC_AWW_Call_Data_Inbound.parquet')
# print(df.head(10))



# Time stamp
current_ts = datetime.datetime.utcnow()
reading_timestamp = current_ts.replace(tzinfo=None)
reading_datetime = current_ts.strftime('%Y-%m-%d %H:%M:%S')

# S3 bucket names and folders
src_bucket = "amwater-bigdata-landing-prod"
trg_bucket = "amwater-bigdata-warehousing-prod"
src_key_prefix = "GC_Services/"
trg_key_prefix = ""
target_file_name = ""

# Copying file from Source S3 bucket
try:
  print("Opening Source Bucket...")
  s3_client = boto3.client('s3')
  s3_resource = boto3.resource('s3')
  for key_dict in s3_client.list_objects(Bucket=src_bucket, Prefix=src_key_prefix)['Contents']:
    file_name = key_dict['Key']
    file_name_new = file_name.replace(src_key_prefix, trg_key_prefix)
    if '.csv' in file_name:
        target_file_name = file_name_new.replace('.csv','')
        print("Downloading {0} now.....".format(file_name_new))
        s3_resource.Bucket(name=src_bucket).download_file(file_name, file_name_new)
        print('Download Complete')
    else:
        continue

except Exception as e:
  print("Exception Occurred: error while copying files to landing")
  print(e)

# Reading the file in pandas data frame
print('Reading the {0} file'.format(target_file_name+'.csv'))
DF = pd.read_csv(target_file_name +'.csv')

# Introducing a new column " processing_time"
print('Adding new column Processing Time')
DF['processing_time'] = reading_datetime

# Changing Schema
lower_case_columns = []
for col in DF.columns:
  lower_case_columns.append(col.lower())
DF.columns = lower_case_columns

# Converting the data types of mixed data type columns
DF['user_id'] = DF['user_id'].astype(str)
DF['ani'] = DF['ani'].astype(str)
DF['date'] = pd.to_datetime(DF['date'],format="%m/%d/%Y")
DF['date'] = pd.to_datetime(DF['date'],unit='s')
DF['date'] = DF['date'].dt.date
#DF['processing_time'] = pd.to_datetime(DF['processing_time'],format='%Y-%m-%d %H:%M:%S')
DF['processing_time'] = (pd.to_datetime(DF['processing_time'],unit='ns'))
print(DF.head(10))
#print(DF.info())
#Saving the file
# print('Saving the {0} file'.format(target_file_name+'.parquet'))
# DF.to_parquet(target_file_name +'.parquet')

# # Uploading the parquet file to target Bucket
# try:
#   s3_client = boto3.client('s3')
#   s3_resource = boto3.resource('s3')
#   # tc = boto3.s3.transfer.TransferConfig()
#   s3_client.upload_file(target_file_name +'.parquet', trg_bucket, 'gc_services'+'/'+target_file_name +'.parquet',ExtraArgs={"ServerSideEncryption": "AES256"})
#   print('Uploaded {0} file succesfully'.format(target_file_name+'.parquet'))
# except Exception as e:
#   print("Exception Occurred: error while copying files to landing")
#   print(e)
 
# # Deleting the temp files
# print('Deleting Temporary Files')
# os.remove(target_file_name +'.csv')
# os.remove(target_file_name +'.parquet')



