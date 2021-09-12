import sys
import json
import boto3
import psycopg2
import itertools
import pandas as pd

# AWS_ACCESS_KEY_ID="ASIAVZ4JLMYPGMEVHVFG"
# AWS_SECRET_ACCESS_KEY="cgAgWUhn/wvD0YZGEHCcznMdqFnvLCCBjRg4UFTm"
# AWS_SESSION_TOKEN="IQoJb3JpZ2luX2VjEG8aCXVzLWVhc3QtMSJHMEUCIQCXhrQ9kWpG5KG0r406icDTWgQB/fGxoIeA2Xe/FBe3tQIgFlF5/G67EmY0I/oAg3jUwR5Mkz+KSW/AaHN0ziGRLJgqmQMIGBACGgwzOTkxODMxNDQ0NzgiDEMwlrfTyb1pcqgnwCr2Aku+UJRcHdZYPkvA1WTkh4jdUznZNdwIjMN1p0LjkGYayZ0SGvc88gjAmWj8e4AQ3A7+YeNcacYxs5oP2h/mkrndyIz0Lm3xHfBQRGM1uJciRrTa/NRXhZQ8Z8Qc81mCQVZ2niVE+dqJNwnVTDVW/sdn6AvAMC3v6wIrZMy+fhI9njRqYwYOzzFc9NfUobuCm4u/oSbPXhxHFPf1qKeGvb8dtLHSGfuCmMFhp0gteeG70rDp8b1e/ZirNa5ijf9JKYVWA7kNepMcyKwvX1MhOPSD43DIVss/m1WiQN9YKxUeKPQndEAlVPYuIObxkZZQBxc7qVHtfasyf5aRM4QyoOzOfwgBhLVWeJoLndI4hGADJqssc4EiraahpulVQ8MuNPptBjsrBPDMQU+r/T2f1KJ7YKAA+JpIXZjJf1fKNzmG3yIwqUvGl3KPdcFsckWbiHgK6I2GivFhY754JLaD6m+arMafSPplxH2SHBTOkIjUQnYcoPoxMM2etIUGOqYBduVBrIzgkfoBiHQxcpsmLbe69mC1iOYcgDP/s6wpgBHxRAgC9X630lUDyYHB+xhs+oi9pGOVzAxDD0Lc5KDBKDxib6VAtOmFRejxHcV8oUxj3M85MWbGzjQGseR+4HH7LXbTDgh3EqPdphohN90Fqhl6SguG5NUcQdBfRgNgcK1pELAyfJFA4xLpD/dVV3XEerhDsclyznBi32mOx0ze7Ny2r9u/ig=="

# client = boto3.client('quicksight', aws_access_key_id = AWS_ACCESS_KEY_ID, aws_secret_access_key= AWS_SECRET_ACCESS_KEY, aws_session_token = AWS_SESSION_TOKEN)
client = boto3.client('quicksight', region_name='us-east-1')
conn = psycopg2.connect(dbname='analytics', host='amwater-bigdata-redshift-prod.ceqr24sy0pgc.us-east-1.redshift.amazonaws.com', port='5439', user='data_admin', password='Admin@123')
cur = conn.cursor();

#to store data of data sets, data set permissions, data set users, and data set details
v_datasets = []
v_dataset_permissions = []
v_dataset_users = []
v_dataset_details = []

#for use of data sets
data_sets = client.list_data_sets(
    AwsAccountId='399183144478',
    MaxResults=100
)

#including fields of arn, datasetid, name, createdtime, lastupdatedtime, importmode
length = len(data_sets['DataSetSummaries'])
y = [(member['Arn'],member['DataSetId'],member['Name'], member['CreatedTime'],member['LastUpdatedTime'],member['ImportMode']) for member in data_sets["DataSetSummaries"] ]
#adding data into v_datasets
v_datasets.extend(y)

#check if next set of results exists in list_data_sets
while 'NextToken' in data_sets:
        data_sets = client.list_data_sets(
            AwsAccountId='399183144478',
            MaxResults=100,
            NextToken=data_sets['NextToken']
            )
        #including fields of arn, datasetid, name, createdtime, lastupdatedtime, importmode
        length +=  len(data_sets['DataSetSummaries'])
        y = [(member['Arn'],member['DataSetId'],member['Name'], member['CreatedTime'],member['LastUpdatedTime'],member['ImportMode']) for member in data_sets["DataSetSummaries"]]
        #adding data into v_datasets
        v_datasets.extend(y)
        
for dataset in v_datasets:
    #some IDs will raise errors hence use try
    try:
        #for use of data set permissions and data set users
        dataset_permissions = client.describe_data_set_permissions(
            AwsAccountId='399183144478',
            DataSetId=dataset[1]
        )

        datasetId=dataset_permissions['DataSetId']
        datasetArn=dataset_permissions['DataSetArn']

        for permission in dataset_permissions['Permissions']:
            principal=permission['Principal']
            #including fields of datasetid, datasetname, principal, permission
            if 'quicksight:UpdateDataSet' in permission["Actions"]:
                x = [(datasetId,dataset[2],principal,"Owner/Co-owner")]
            elif 'quicksight:DescribeDataSet' in permission["Actions"]:
                x = [(datasetId,dataset[2],principal,"Viewer")]
            #adding data into v_dataset_users
            v_dataset_users.extend(x)	
            #including fields of datasetid, datasetarn, principal, action
            z = [(datasetId,datasetArn,principal,Action) for Action in permission["Actions"]]
            #adding data into v_dataset_permissions
            v_dataset_permissions.extend(z)
    except Exception as e:
        print(e)

for i in range(length):
    #some IDs will raise errors hence use try
    try:
        #for use of data set details
        dataset_details = client.describe_data_set(
            AwsAccountId='399183144478',
            DataSetId=v_datasets[i][1]
        )   
        
        member=dataset_details["DataSet"]


        #to store the keys inside dataset_details["DataSet"]["PhysicalTableMap"]
        keylist=[]
        if 'PhysicalTableMap' in member.keys():
            keylist.extend(member['PhysicalTableMap'].keys())
        
        if len(keylist) > 0:

            for j in range(len(keylist)):

                relationalTableInputColumns=member['PhysicalTableMap'][keylist[j]]['RelationalTable']['InputColumns'] if ('RelationalTable' in member['PhysicalTableMap'][keylist[j]].keys()) else ''
                customSqlColumns=member['PhysicalTableMap'][keylist[j]]['CustomSql']['Columns'] if ('CustomSql' in member['PhysicalTableMap'][keylist[j]].keys()) else ''
                s3SourceInputColumns=member['PhysicalTableMap'][keylist[j]]['S3Source']['InputColumns'] if ('S3Source' in member['PhysicalTableMap'][keylist[j]].keys()) else ''
                outputColumns=member['OutputColumns'] if ('OutputColumns' in member.keys()) else ''
                columnGroups=member['ColumnGroups'] if ('ColumnGroups' in member.keys()) else ''
                fieldFoldersColumns=member['FieldFolders'][keylist[j]]['columns'] if ('FieldFolders' in member.keys()) else ''
                #including fields of DataSetId, Arn, Name, CreatedTime, LastUpdatedTime, PhysicalTableMap_RelationalTable_DataSourceArn, PhysicalTableMap_RelationalTable_Catalog, PhysicalTableMap_RelationalTable_Schema, PhysicalTableMap_RelationalTable_Name, PhysicalTableMap_RelationalTable_InputColumns_Name, PhysicalTableMap_RelationalTable_InputColumns_Type, PhysicalTableMap_CustomSql_DataSourceArn, PhysicalTableMap_CustomSql_Name, PhysicalTableMap_CustomSql_SqlQuery, PhysicalTableMap_CustomSql_Columns_Name, PhysicalTableMap_CustomSql_Columns_Type, PhysicalTableMap_S3Source_DataSourceArn, PhysicalTableMap_S3Source_UploadSettings_Format, PhysicalTableMap_S3Source_UploadSettings_StartFromRow, PhysicalTableMap_S3Source_UploadSettings_ContainsHeader, PhysicalTableMap_S3Source_UploadSettings_TextQualifier, PhysicalTableMap_S3Source_UploadSettings_Delimiter, PhysicalTableMap_S3Source_InputColumns_Name, PhysicalTableMap_S3Source_InputColumns_Type, OutputColumns_Name, OutputColumns_Description, OutputColumns_Type, ImportMode, ConsumedSpiceCapacityInBytes, ColumnGroups_GeoSpatialColumnGroup_Name, ColumnGroups_GeoSpatialColumnGroup_CountryCode, ColumnGroups_GeoSpatialColumnGroup_Columns, FieldFolders_description, FieldFolders_columns
                w = [(member['DataSetId'],member['Arn'],member['Name'],member['CreatedTime'],member['LastUpdatedTime'],
                member['PhysicalTableMap'][keylist[j]]['RelationalTable']['DataSourceArn'] if ('RelationalTable' in member['PhysicalTableMap'][keylist[j]] and 'DataSourceArn' in member['PhysicalTableMap'][keylist[j]]['RelationalTable'].keys()) else "",
                member['PhysicalTableMap'][keylist[j]]['RelationalTable']['Catalog'] if ('RelationalTable' in member['PhysicalTableMap'][keylist[j]] and 'Catalog' in member['PhysicalTableMap'][keylist[j]]['RelationalTable'].keys()) else "",
                member['PhysicalTableMap'][keylist[j]]['RelationalTable']['Schema'] if ('RelationalTable' in member['PhysicalTableMap'][keylist[j]] and 'Schema' in member['PhysicalTableMap'][keylist[j]]['RelationalTable'].keys()) else "",
                member['PhysicalTableMap'][keylist[j]]['RelationalTable']['Name'] if ('RelationalTable' in member['PhysicalTableMap'][keylist[j]] and 'Name' in member['PhysicalTableMap'][keylist[j]]['RelationalTable'].keys()) else "",
                x['Name'] if (x!=None) else '', x['Type'] if (x!=None) else '',
                member['PhysicalTableMap'][keylist[j]]['CustomSql']['DataSourceArn'] if ('CustomSql' in member['PhysicalTableMap'][keylist[j]].keys() and'DataSourceArn' in member['PhysicalTableMap'][keylist[j]]['CustomSql'].keys()) else "",
                member['PhysicalTableMap'][keylist[j]]['CustomSql']['Name'] if ('CustomSql' in member['PhysicalTableMap'][keylist[j]].keys() and 'Name' in member['PhysicalTableMap'][keylist[j]]['CustomSql'].keys()) else "",
                member['PhysicalTableMap'][keylist[j]]['CustomSql']['SqlQuery'] if ('CustomSql' in member['PhysicalTableMap'][keylist[j]].keys() and 'SqlQuery' in member['PhysicalTableMap'][keylist[j]]['CustomSql'].keys()) else "",
                y['Name'] if (y!=None) else '', y['Type'] if (y!=None) else '',
                member['PhysicalTableMap'][keylist[j]]['S3Source']['DataSourceArn'] if ('S3Source' in member['PhysicalTableMap'][keylist[j]].keys() and 'DataSourceArn' in member['PhysicalTableMap'][keylist[j]]['S3Source'].keys()) else "",
                member['PhysicalTableMap'][keylist[j]]['S3Source']['UploadSettings']['Format'] if ('S3Source' in member['PhysicalTableMap'][keylist[j]].keys() and 'Format' in member['PhysicalTableMap'][keylist[j]]['S3Source']['UploadSettings'].keys()) else "",
                member['PhysicalTableMap'][keylist[j]]['S3Source']['UploadSettings']['StartFromRow'] if ('S3Source' in member['PhysicalTableMap'][keylist[j]].keys() and 'StartFromRow' in member['PhysicalTableMap'][keylist[j]]['S3Source']['UploadSettings'].keys()) else "",
                member['PhysicalTableMap'][keylist[j]]['S3Source']['UploadSettings']['ContainsHeader'] if ('S3Source' in member['PhysicalTableMap'][keylist[j]].keys() and 'ContainsHeader' in member['PhysicalTableMap'][keylist[j]]['S3Source']['UploadSettings'].keys()) else False,
                member['PhysicalTableMap'][keylist[j]]['S3Source']['UploadSettings']['TextQualifier'] if ('S3Source' in member['PhysicalTableMap'][keylist[j]].keys() and 'TextQualifier' in member['PhysicalTableMap'][keylist[j]]['S3Source']['UploadSettings'].keys()) else "",
                member['PhysicalTableMap'][keylist[j]]['S3Source']['UploadSettings']['Delimiter'] if ('S3Source' in member['PhysicalTableMap'][keylist[j]].keys() and 'Delimiter' in member['PhysicalTableMap'][keylist[j]]['S3Source']['UploadSettings'].keys()) else "",
                z['Name'] if (z!=None) else '', z['Type'] if (z!=None) else '',
                a['Name'] if (a!=None and 'Name' in a.keys()) else '',
                a['Description'] if (a!=None and 'Description' in a.keys()) else '',
                a['Type'] if (a!=None and 'Type' in a.keys()) else '',
                member['ImportMode'] if ('ImportMode' in member.keys()) else '', 
                member['ConsumedSpiceCapacityInBytes'] if ('ConsumedSpiceCapacityInBytes' in member.keys()) else '',
                b['GeoSpatialColumnGroup']['Name'] if (b!=None and 'Name' in b['GeoSpatialColumnGroup'].keys()) else '',
                b['GeoSpatialColumnGroup']['CountryCode'] if (b!=None and 'CountryCode' in b['GeoSpatialColumnGroup'].keys()) else '',
                b['GeoSpatialColumnGroup']['Columns'] if (b!=None and 'Columns' in b['GeoSpatialColumnGroup'].keys()) else '',
                member['FieldFolders'][keylist[j]]['description'] if ('FieldFolders' in member.keys()) else "",
                member['FieldFolders'][keylist[j]]['columns'] if ('FieldFolders' in member.keys()) else "",
                ) for x,y,z,a,b in itertools.zip_longest(relationalTableInputColumns,customSqlColumns,s3SourceInputColumns,outputColumns,columnGroups)]
                
        else:
            # Updating the dataset ids without PhysicalTableMap Keys
            w = [(member['DataSetId'],member['Arn'],member['Name'],member['CreatedTime'],member['LastUpdatedTime'],"","","","","","","","","",'','', "","",
                    "",False,"","",'','','','','','', '','','','',"","")]
            
    
    #add data into v_dataset_details
        v_dataset_details.extend(w)
        
    except Exception as e:
        # Dataset Id's Failed in Describe_Dataset API call
        w = [(v_datasets[i][1],v_datasets[i][0],v_datasets[i][2],v_datasets[i][3],v_datasets[i][4],"","","","","","","","","",'','', "","",
                    "",False,"","",'','','','','','', '','','','',"","")]
        v_dataset_details.extend(w)

print("Number of Datasets: " + str(len(v_datasets)))
print("Number of dataset_users: " + str(len(v_dataset_users)))
print("Number of dataset_permissions: " + str(len(v_dataset_permissions)))
print("Number of dataset_details: " + str(len(v_dataset_details)))


# Checking Unique Dataset Id's in dataset_details
df = pd.DataFrame(v_dataset_details,columns = ['DataSetId', 'Arn', 'Name', 'CreatedTime', 'LastUpdatedTime', 'PhysicalTableMap_RelationalTable_DataSourceArn', 'PhysicalTableMap_RelationalTable_Catalog', 'PhysicalTableMap_RelationalTable_Schema', 'PhysicalTableMap_RelationalTable_Name', 'PhysicalTableMap_RelationalTable_InputColumns_Name', 'PhysicalTableMap_RelationalTable_InputColumns_Type', 'PhysicalTableMap_CustomSql_DataSourceArn', 'PhysicalTableMap_CustomSql_Name', 'PhysicalTableMap_CustomSql_SqlQuery', 'PhysicalTableMap_CustomSql_Columns_Name', 'PhysicalTableMap_CustomSql_Columns_Type', 'PhysicalTableMap_S3Source_DataSourceArn', 'PhysicalTableMap_S3Source_UploadSettings_Format', 'PhysicalTableMap_S3Source_UploadSettings_StartFromRow', 'PhysicalTableMap_S3Source_UploadSettings_ContainsHeader', 'PhysicalTableMap_S3Source_UploadSettings_TextQualifier', 'PhysicalTableMap_S3Source_UploadSettings_Delimiter', 'PhysicalTableMap_S3Source_InputColumns_Name', 'PhysicalTableMap_S3Source_InputColumns_Type', 'OutputColumns_Name', 'OutputColumns_Description', 'OutputColumns_Type', 'ImportMode', 'ConsumedSpiceCapacityInBytes', 'ColumnGroups_GeoSpatialColumnGroup_Name', 'ColumnGroups_GeoSpatialColumnGroup_CountryCode', 'ColumnGroups_GeoSpatialColumnGroup_Columns', 'FieldFolders_description', 'FieldFolders_columns'])
print(len(df['DataSetId'].unique()))

#ingest data into redshift quicksight_admin schema
cur.execute("truncate table quicksight_admin.data_sets")
cur.execute("truncate table quicksight_admin.dataset_permissions")
cur.execute("truncate table quicksight_admin.dataset_users")   
cur.execute("truncate table quicksight_admin.dataset_details")
datasets_query = """insert into quicksight_admin.data_sets (arn, datasetid, name, createdtime, lastupdatedtime, importmode, processingtime) VALUES (%s,%s,%s,%s,%s,%s,getdate());"""
datasets_permissions_query = """insert into quicksight_admin.dataset_permissions (datasetid, datasetarn, principal, action, processingtime) VALUES (%s,%s,%s,%s,getdate());"""
datasets_users_query = """insert into quicksight_admin.dataset_users (datasetid, datasetname, principal, permission, processingtime) VALUES (%s,%s,%s,%s,getdate());"""
datasets_details_query = """insert into quicksight_admin.dataset_details (DataSetId, Arn, Name, CreatedTime, LastUpdatedTime, PhysicalTableMap_RelationalTable_DataSourceArn, PhysicalTableMap_RelationalTable_Catalog, PhysicalTableMap_RelationalTable_Schema, PhysicalTableMap_RelationalTable_Name, PhysicalTableMap_RelationalTable_InputColumns_Name, PhysicalTableMap_RelationalTable_InputColumns_Type, PhysicalTableMap_CustomSql_DataSourceArn, PhysicalTableMap_CustomSql_Name, PhysicalTableMap_CustomSql_SqlQuery, PhysicalTableMap_CustomSql_Columns_Name, PhysicalTableMap_CustomSql_Columns_Type, PhysicalTableMap_S3Source_DataSourceArn, PhysicalTableMap_S3Source_UploadSettings_Format, PhysicalTableMap_S3Source_UploadSettings_StartFromRow, PhysicalTableMap_S3Source_UploadSettings_ContainsHeader, PhysicalTableMap_S3Source_UploadSettings_TextQualifier, PhysicalTableMap_S3Source_UploadSettings_Delimiter, PhysicalTableMap_S3Source_InputColumns_Name, PhysicalTableMap_S3Source_InputColumns_Type, OutputColumns_Name, OutputColumns_Description, OutputColumns_Type, ImportMode, ConsumedSpiceCapacityInBytes, ColumnGroups_GeoSpatialColumnGroup_Name, ColumnGroups_GeoSpatialColumnGroup_CountryCode, ColumnGroups_GeoSpatialColumnGroup_Columns, FieldFolders_description, FieldFolders_columns, processingtime) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,getdate());"""

cur.executemany(datasets_users_query, v_dataset_users)
cur.executemany(datasets_query, v_datasets)
cur.executemany(datasets_permissions_query, v_dataset_permissions)
cur.executemany(datasets_details_query, v_dataset_details)

conn.commit()
cur.close()
conn.close()
