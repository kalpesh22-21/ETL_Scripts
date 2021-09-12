import pyodbc
import snowflake.connector
import pandas as pd
import math
import yaml
import os
from datetime import datetime
import glob


class Etl:
    def __init__(self, config_path, data_path):
        self.config_path = config_path  # config file path to be given as a parameter
        self.data_path = data_path + '{tableName}\\'

    # Extracts the data for a table from the ETL server
    def extractData(self, table_name):
        try:
            with open(self.config_path, encoding='utf-8') as file:  # opening the config file
                con_list = yaml.load(file, Loader=yaml.FullLoader)  # dictionary with credentials

            tableName = table_name

            start_time = datetime.today()  # getting the start time of job for updating the job status table

            # Establishing connection with Snowflake
            ctx = snowflake.connector.connect(
                user=con_list['snowflake_conn']['user'],
                password=con_list['snowflake_conn']['password'],
                account=con_list['snowflake_conn']['account'])

            cs = ctx.cursor()

            # retrieving the latest extract start time for delta tables
            cs.execute(
                "select max(EXTRACT_START_TIME) from " + con_list['snowflake_conn']['database'] + ".LANDING.JOB_STATUS_TABLE where JOB_NAME='{tableName}' and EXTRACT_STATUS='SUCCESS' and LANDING_STATUS='SUCCESS' and STAGE_STATUS='SUCCESS'".format(
                    tableName=tableName))

            last_time = '1900-01-01'
            for row in cs.fetchall():
                last_time = row[0]

            # connecting with ETL server
            conn = pyodbc.connect(
                'DSN=' + con_list['extractdata']['DSN'] + ';UID=' + con_list['extractdata']['UID'] + ';PWD=' +
                con_list['extractdata']['PWD'])
            cursor = conn.cursor()

            # Check if there is a trigger or custom query for any table mentioned in the config file
            if con_list['extractdata'][tableName]['query'] is None:
                sqlTbl = "select * from " + con_list['extractdata']['schema'] + ".{tableName} limit {limit} offset {offset}"
            else:
                sqlTbl = con_list['extractdata'][tableName]['query'] + " limit {limit} offset {offset}"

            # Getting  the number of records the query is retrieving.
            if con_list['extractdata'][tableName]['query'] is None:
                cursor.execute("select count(*) from " + con_list['extractdata']['schema'] + ".{tableName}".format(
                    tableName=tableName))
                for row in cursor.fetchall():
                    noOfRcrds = row[0]
            else:
                cursor.execute("select count(*) from ( " + con_list['extractdata'][tableName]['query'].format(
                    last_runtime=last_time) + ")")
                for row in cursor.fetchall():
                    noOfRcrds = row[0]

            print("no of records ", noOfRcrds)
            noOfPart = con_list['extractdata'][tableName]['partition']  # the number of partitions in which the files must be divided.

            # If there are different number of partitions next time for the same table then deleting the previous data files from the folder
            if os.path.isdir(self.data_path[:-1].format(tableName=tableName)):
                number_files = len(os.listdir(self.data_path.format(tableName=tableName)))  # folder path
                if noOfPart != number_files:
                    files = glob.glob((self.data_path + '*').format(tableName=tableName))
                    for f in files:
                        os.remove(f)

            if noOfRcrds <= 1000:
                noOfPart = 1
            elif noOfRcrds <= 10000:
                noOfPart = 10

            print("No of partitions ", noOfPart)

            # Doing integer division to get the number of records for a particular partition.
            limit = math.floor(noOfRcrds / noOfPart)
            mod = noOfRcrds % noOfPart  # the remainder
            last_limit = mod + limit  # the remainder + the limit value to be taken as limit for last partition records

            for i in range(noOfPart):
                # For the last partition the limit value will change as it might increase.
                if i == noOfPart - 1:
                    offset = i * limit  # setting the offset value
                    if con_list['extractdata'][tableName]['query'] is None:
                        sql = sqlTbl.format(tableName=tableName, limit=last_limit, offset=offset)
                    else:
                        sql = sqlTbl.format(last_runtime=last_time, limit=last_limit, offset=offset)
                    print(sql)

                    # Making pandas dataframe of the sql.
                    df = pd.read_sql(sql, conn)

                    # Exporting the csv file to the desired data location
                    df.to_csv((self.data_path + '{tableName}-{partition}.csv').format(tableName=tableName, partition=i),
                              index=False, header=False)
                else:  # For partitions except the last one limit will remain same
                    offset = i * limit  # setting the offset value
                    if con_list['extractdata'][tableName]['query'] is None:
                        sql = sqlTbl.format(tableName=tableName, limit=limit, offset=offset)
                    else:
                        sql = (con_list['extractdata'][tableName]['query'] + " limit {limit} offset {offset}").format(
                            last_runtime=last_time,
                            limit=limit, offset=offset)
                    print(sql)

                    # Making pandas dataframe of the sql.
                    df = pd.read_sql(sql, conn)

                    # Exporting the csv file to the desired data location
                    df.to_csv((self.data_path + '{tableName}-{partition}.csv').format(tableName=tableName, partition=i),
                              index=False, header=False)

            end_time = datetime.today()  # Getting the job end time for updating job status table

            # Making a new extract success entry for the table in the job status table
            cs.execute(
                "insert into " + con_list['snowflake_conn']['database'] + ".LANDING.JOB_STATUS_TABLE(JOB_NAME, EXTRACT_START_TIME, EXTRACT_END_TIME, EXTRACT_STATUS) values('{tableName}','{start_time}', '{end_time}', 'SUCCESS')".format(
                    tableName=tableName, start_time=start_time, end_time=end_time))
            print("EXTRACT SUCCESS")
        except Exception as e:  # Error handling
            print("EXTRACT FAILED")
            print(e)  # stating the error

            # Making a new extract fail job entry for the table in the job status table
            cs.execute(
                "insert into " + con_list['snowflake_conn']['database'] + ".JOB_STATUS_TABLE(JOB_NAME, EXTRACT_START_TIME, EXTRACT_END_TIME, EXTRACT_STATUS) values('{tableName}','{start_time}', NULL, 'FAIL')".format(
                    tableName=tableName, start_time=start_time))
        finally:  # closing the connection
            ctx.close()
            conn.close()

    # Loads extracted data in the respective table in landing schema
    def loadSnowflake(self, table_name):
        try:
            with open(self.config_path, encoding='utf-8') as file:  # opening the config file
                con_list = yaml.load(file, Loader=yaml.FullLoader)  # dictionary with credentials

            tableName = table_name

            # Establishing connection with Snowflake
            ctx = snowflake.connector.connect(
                user=con_list['snowflake_conn']['user'],
                password=con_list['snowflake_conn']['password'],
                account=con_list['snowflake_conn']['account'])

            cs = ctx.cursor()

            start_time = datetime.today()  # getting the start time of job for updating the job status table

            ctx.cursor().execute("USE DATABASE " + con_list['snowflake_conn']['database'])
            ctx.cursor().execute("USE SCHEMA LANDING")

            # Using the put command to load files in the Snowflake stage.
            ctx.cursor().execute(
                ("PUT file://" + self.data_path + "* @%{tableName} auto_compress=true overwrite=true").format(
                    tableName=tableName))

            # Checking if there are any records which will fail while loading data in table
            cs.execute("copy into {tableName} FILE_FORMAT = (format_name = 'csv_landing') validation_mode='RETURN_ALL_ERRORS'".format(tableName=tableName))

            # If there are failed records then inserting them in a table for failed records to later check and correct
            # the error and load again
            if cs.rowcount != 0:
                cs.execute("select last_query_id()")
                for row in cs.fetchall():
                    query_id = str(row[0])
                cs.execute("show tables like 'FAILED_RECORDS'")
                if cs.rowcount == 0:  # If the table does not exist then creating the table as well as inserting the failed records details
                    cs.execute(
                        "create or replace table LANDING.FAILED_RECORDS as select ERROR, FILE, LINE, REJECTED_RECORD, '{tableName}' as TABLE_NAME, current_timestamp as FAILED_DATE from table(result_scan('{query_id}'))".format(
                            query_id=query_id, tableName=tableName))
                else:  # If the table exists then inserting the failed records details
                    cs.execute(
                        "insert into LANDING.FAILED_RECORDS select ERROR, FILE, LINE, REJECTED_RECORD, '{tableName}' as TABLE_NAME, current_timestamp as FAILED_DATE from table(result_scan('{query_id}'))".format(
                            query_id=query_id, tableName=tableName))

            # Landing table is always truncate and load
            ctx.cursor().execute("truncate table {tableName}".format(tableName=tableName))

            # loading in the landing table
            ctx.cursor().execute(
                "COPY INTO {tableName} FILE_FORMAT = (format_name = 'csv_landing') ON_ERROR = CONTINUE".format(
                    tableName=tableName))

            end_time = datetime.today()  # Getting the job end time for updating job status table

            # Updating the job status table with SUCCESS status for the corresponding extract job
            ctx.cursor().execute(
                "update JOB_STATUS_TABLE set LANDING_START_TIME='{start_time}',LANDING_END_TIME='{end_time}',LANDING_STATUS='SUCCESS' where JOB_NAME='{tableName}' and EXTRACT_END_TIME=(select max(EXTRACT_END_TIME) from JOB_STATUS_TABLE where JOB_NAME='{tableName}') and EXTRACT_STATUS='SUCCESS'".format(
                    tableName=tableName, start_time=start_time, end_time=end_time))
            print("LANDING LOAD SUCCESS")
        except Exception as e:
            print("LANDING LOAD FAILED")
            print(e)  # Stating the error

            # Updating the job status table with FAIL status for the corresponding extract job
            ctx.cursor().execute(
                "update JOB_STATUS_TABLE set LANDING_START_TIME='{start_time}',LANDING_STATUS='FAIL' where JOB_NAME='{tableName}' and EXTRACT_END_TIME=(select max(EXTRACT_END_TIME) from JOB_STATUS_TABLE where JOB_NAME='{tableName}') and EXTRACT_STATUS='SUCCESS'".format(
                    tableName=tableName, start_time=start_time))
        finally:  # Closing the connection
            ctx.close()

    # Loads landing table data in respective table with the respective load strategy mentioned in reference table in stage schema
    def loadStage(self, table_name):
        try:
            with open(self.config_path, encoding='utf-8') as file:  # opening the config file
                con_list = yaml.load(file, Loader=yaml.FullLoader)  # dictionary with credentials

            tableName = table_name

            # Establishing connection with Snowflake
            ctx = snowflake.connector.connect(
                user=con_list['snowflake_conn']['user'],
                password=con_list['snowflake_conn']['password'],
                account=con_list['snowflake_conn']['account']
                )

            cs = ctx.cursor()

            start_time = datetime.today()  # getting the start time of job for updating the job status table

            ctx.cursor().execute("USE DATABASE " + con_list['snowflake_conn']['database'])
            ctx.cursor().execute("USE SCHEMA LANDING")

            # Retrieving the destination table name from reference table for taking stage before and after row count
            cs.execute("select destination_table from reference_table where tablename='{tableName}'".format(tableName=tableName))
            for row in cs.fetchall():
                dest_table = str(row[0])

            # Number of records in stage table before running the load job
            cs.execute("select count(*) from STAGE.{dest_table}".format(dest_table=dest_table))
            for row in cs.fetchall():
                count_before = int(row[0])

            # Executing the Snowflake stored procedure which loads data in stage table according to the load strategy
            # mentioned in the reference table(TYPE_2, UPSERT, INSERT)
            cs.execute("call sp_load_stage('{tableName}')".format(tableName=tableName))
            end_time = datetime.today()  # getting the end time of job for updating the job status table
            for row in cs.fetchall():
                # If the stored procedure is executed successfully then updating the job status table with SUCCESS status
                # for the corresponding landing load job
                if str(row[0]).upper() in ["TYPE_2 SUCCESSFUL", "SUCCESSFUL"]:
                    cs.execute("update JOB_STATUS_TABLE set STAGE_START_TIME='{start_time}', STAGE_END_TIME='{end_time}', STAGE_STATUS='SUCCESS', STAGE_COUNT_BEFORE={count_before}, STAGE_COUNT_AFTER=(select count(*) from stage.{dest_table}) where JOB_NAME='{tableName}' and LANDING_END_TIME=(select max(LANDING_END_TIME) from JOB_STATUS_TABLE where JOB_NAME='{tableName}')".format(start_time=start_time,end_time=end_time,count_before=count_before,dest_table=dest_table,tableName=tableName))

                # If the stored procedure is executed successfully then updating the job status table with FAIL status
                # for the corresponding landing load job
                else:
                    cs.execute(
                        "update JOB_STATUS_TABLE set STAGE_START_TIME='{start_time}', STAGE_END_TIME=null, STAGE_STATUS='FAIL', STAGE_COUNT_BEFORE={count_before}, STAGE_COUNT_AFTER=(select count(*) from stage.{dest_table}) where JOB_NAME='{tableName}' and LANDING_END_TIME=(select max(LANDING_END_TIME) from JOB_STATUS_TABLE where JOB_NAME='{tableName}')".format(
                            start_time=start_time, count_before=count_before, dest_table=dest_table,
                            tableName=tableName))
                    raise Exception(str(row[0]))  # raise error with the error returned by stored procedure
            print("STAGE LOAD SUCCESS")
        except Exception as e:
            print("STAGE LOAD FAILED")
            print(e)  # Stating the error
        finally:  # Closing the connection
            ctx.close()
