# Importing Libraries
import sys
import boto3
import datetime
import pandas as pd
from zipfile import ZipFile
import os

# AWS_ACCESS_KEY_ID="ASIAVZ4JLMYPERWAP747"
# AWS_SECRET_ACCESS_KEY="K4O+EqQKueyrWmgrpI6u2VE4wNh1BdOc6N5DSIbH"
# AWS_SESSION_TOKEN="IQoJb3JpZ2luX2VjEOX//////////wEaCXVzLWVhc3QtMSJGMEQCIE34veBchTkV5IZlkMyJn+PMghdIAoQKbPgq0lxtZuGLAiArz2H+WS0vPq/RKBLScpBf38LhB/5EQjLZY/ln4CLRpyqiAwi+//////////8BEAIaDDM5OTE4MzE0NDQ3OCIMvChcOT+j9fjoB/LuKvYC0o2B3EnM2Cd86ZGVVivIjHATtR53vwOYp4gP3gfg71KZ8egy62wzpDFy2hK2f6VOTNHUXVQ7zWdVRc0u3TukQmng6QSckbos3DBUOGrjvrMRTTLNwYGvkuQC4ZtLVqytXdxEufHp3oc/pDR9PqryFOe4YB7Doa5LPeJVw3ZuIeDjHNT0parqvS8FnWb9aeTPXSfaS2h7PE8WLd7GGkTyPCBOUyKf9I24Dydb9M+9ouabjai+vsj+evHxwf3v8m08Ve/7clfAkDPEYCU4S5JhKdTbvpvW1i3IQyn6szrbVXSXkDQ0X/jLK0sA5WtjvWUomCwE7Qu8xQQu3oGGb5wsjNUaGyfwBOxc+je+1Tyz0WRPlqTj9cNNXwJtk+/GgssjoGWEo8etPfIr57ScogZoDQKXQ22Eip3STPywZpeB8BszeVDr7G9txMgSepTE5vpm1B2Zm/icaksyn+jGYeP5T4bWQPnwQUVq3q2apuP7ZB6/wNQtEGow+vH2hgY6pwFyk4tr0h5hx6s7b6yohegXQEZft0VFBGiiZLZc1b8QFtjHVTegcaMwHmV2DRCdaycanv8dgVagZWeDfDioUjybvevgcZawRQcxdTRDBIaxlJlussrCd4O2qm5FVgnAJM8J2B64LJpgSn/0rH5SmmhsmaOCt74EMygd4MRhhooWRr9HGOxkOg9blJvR/kif6G/Bg62gZkxM84Bko2ysEHiyFyq60Ymcug=="


# S3 bucket names and folders
src_bucket = "aw-mft-bigdata-prod"
landing_bucket = "amwater-bigdata-landing-prod"
trg_bucket = "amwater-bigdata-warehousing-prod"
src_key_prefix = "EOS/"
landing_key_prefix = "EOS/"
move_landing_prefix = "archive/EOS/"
target_key_prefix = "eos/"

# s3_client = boto3.client('s3', aws_access_key_id = AWS_ACCESS_KEY_ID, aws_secret_access_key= AWS_SECRET_ACCESS_KEY, aws_session_token = AWS_SESSION_TOKEN)
# s3_resource = boto3.resource('s3', aws_access_key_id = AWS_ACCESS_KEY_ID, aws_secret_access_key= AWS_SECRET_ACCESS_KEY, aws_session_token = AWS_SESSION_TOKEN)

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

# Copying file from Source S3 bucket
try:
  print("Copying Files From Source to Landing...")
  # Iterating over the files in source Bucket
  for key_dict in s3_client.list_objects(Bucket=src_bucket, Prefix=src_key_prefix)['Contents']:
    # Getting the Directory of the file
    file_name = key_dict['Key']
    # Name of the file required in landing bucket
    file_name_landing = file_name.replace(src_key_prefix, '')
    if '.csv' in file_name:
      try:
        s3_client.head_object(Bucket= landing_bucket , Key= move_landing_prefix + '2021/'+ file_name_landing )
      except:
        print('Copying {0} to landing.....'.format(file_name_landing))
        # Downloading the file on the local
        s3_resource.Bucket(name=src_bucket).download_file(file_name, file_name_landing)
        # Uploading the file Landing bucket in 2021 folder as all csv files are of 2021 year
        s3_client.upload_file(file_name_landing, landing_bucket, landing_key_prefix + '2021/' +file_name_landing,ExtraArgs={"ServerSideEncryption": "AES256"})
        # Removing Temporary downloded file
        os.remove(file_name_landing)
    elif '.zip' in file_name:
      # Using the name of the file to form seperate directory for each year
      year_of_file = ''.join(list(file_name_landing.replace('.zip',''))[-4:])
      # Downloading the file on the local
      s3_resource.Bucket(name=src_bucket).download_file(file_name, file_name_landing)
      print('Extracting and Copying {0} to landing bucket .....'.format(file_name_landing))
      # Extracting the Zip File
      with ZipFile(file_name_landing, 'r') as z:
        listOfFileNames = z.namelist()
        for file in listOfFileNames:
            # Check filename endswith csv
            if file.endswith('.csv'):
              try:
                s3_client.head_object(Bucket= landing_bucket , Key= move_landing_prefix + year_of_file +'/'+ file )
              except:
                print('File {0} is not present thus extracting now...'.format(file))
                # Extract a single file from zip
                z.extract(file)
                # Uploading the file Landing bucket in year_of_file folder
                s3_client.upload_file(file, landing_bucket, landing_key_prefix + year_of_file + '/' +file,ExtraArgs={"ServerSideEncryption": "AES256"})
                # Removing Temporary exctrated file
                os.remove(file)
      # Removing Temporary downloded zip file      
      os.remove(file_name_landing)

except Exception as e:
  print("Exception Occurred: error while copying files to landing")
  print(e)      
  
try:
  print("Coverting Files To Parquet From Landing and copying to Warehousing...")
  # Paging the object as they are more than maximum keys in list_objects
  paginator = s3_client.get_paginator('list_objects_v2')
  pages = paginator.paginate(Bucket=landing_bucket, Prefix=landing_key_prefix)
  # pages = paginator.paginate(Bucket=landing_bucket, Prefix=move_landing_prefix)
  # Iterating over the pages
  
  for page in pages:
    for obj in page['Contents']:
      # Getting the Directory of the file      
      file_name = obj['Key']
      # Name of the file required in warehousing bucket
      file_name_landing = file_name.replace(landing_key_prefix, '')
      # Year of file for forming folder
      Year_of_file = file_name_landing.split('/')[0]
      # String name of the file
      file_name_target = (file_name_landing.split('/')[1]).replace('.csv','')
      # Date of creation of file
      date_of_file = (file_name_landing.split('/')[1]).replace('.csv','').split('_')[2]

      if int(''.join(list(date_of_file)[:4])) > 2020:
        try:
          s3_client.head_object(Bucket= trg_bucket , Key= target_key_prefix + file_name_target +'.parquet' )
          print('{0} already exists in Warehousing Bucket'.format(file_name_target+'.parquet'))
          response = s3_client.delete_object(Bucket= landing_bucket ,Key= file_name)
        except:
          
          # # Downloading file on the local
          s3_resource.Bucket(name=landing_bucket).download_file(file_name, file_name_target+'.csv')
          # Uploading the file in Archive bucket
          s3_client.upload_file(file_name_target + '.csv', landing_bucket, move_landing_prefix + Year_of_file + '/' + file_name_target + '.csv',ExtraArgs={"ServerSideEncryption": "AES256"})
          # Deleting the file from landing after moving to archive
          response = s3_client.delete_object(Bucket= landing_bucket ,Key= file_name)
          print("File Moved To Archive")
          #Reading the csv file
          DF = pd.read_csv(file_name_target + '.csv', sep='|',dtype = 'str')
          # Insering new column with the date of creation
  
          DF['timestamp'] = date_of_file
           
          if len(date_of_file) == 8:
          # Convrting to date time
            DF['timestamp'] = pd.to_datetime(DF['timestamp'],format="%Y%m%d")
          elif len(date_of_file) == 14 :
            DF['timestamp'] = pd.to_datetime(DF['timestamp'],format="%Y%m%d%H%M%S")
          
          # Saving the parquet file
          if DF.empty == False:
            print('Saving and uplaoding the {0} file to Warehousing bucket.....'.format(file_name_target+'.parquet'))
            DF.to_parquet(file_name_target +'.parquet')
             
            s3_client.upload_file(file_name_target +'.parquet', trg_bucket, target_key_prefix +file_name_target +'.parquet',ExtraArgs={"ServerSideEncryption": "AES256"})
             
            os.remove(file_name_target +'.parquet')
            os.remove(file_name_target+'.csv')
          else:
            # print('{0} file is an empty file thus not saved to warehouse.....'.format(file_name_target+'.parquet'))
            os.remove(file_name_target+'.csv')
       
except Exception as e:
  print("Exception Occurred: error when copying files to WareHouse")
  print(e)