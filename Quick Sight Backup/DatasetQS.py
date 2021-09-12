#Done by Leo Li
#Reference QuickSight Documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/quicksight.html#quicksight
import sys
import json
import boto3
import psycopg2
import itertools

client = boto3.client('quicksight')
conn = psycopg2.connect(dbname='analytics', host='amwater-bigdata-redshift-prod.ceqr24sy0pgc.us-east-1.redshift.amazonaws.com', port='5439', user='admin', password='Admin123')
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

        #for use of data set details
        dataset_details = client.describe_data_set(
            AwsAccountId='399183144478',
            DataSetId=dataset[1]
        )   
        member=dataset_details["DataSet"]
        #to store the keys inside dataset_details["DataSet"]["PhysicalTableMap"]
        keylist=[]
        if 'PhysicalTableMap' in member.keys():
            keylist.extend(member['PhysicalTableMap'].keys())
        i=0
        for l1 in keylist:
            relationalTableInputColumns=member['PhysicalTableMap'][keylist[i]]['RelationalTable']['InputColumns'] if ('RelationalTable' in member['PhysicalTableMap'][keylist[i]].keys()) else ''
            customSqlColumns=member['PhysicalTableMap'][keylist[i]]['CustomSql']['Columns'] if ('CustomSql' in member['PhysicalTableMap'][keylist[i]].keys()) else ''
            s3SourceInputColumns=member['PhysicalTableMap'][keylist[i]]['S3Source']['InputColumns'] if ('S3Source' in member['PhysicalTableMap'][keylist[i]].keys()) else ''
            outputColumns=member['OutputColumns'] if ('OutputColumns' in member.keys()) else ''
            columnGroups=member['ColumnGroups'] if ('ColumnGroups' in member.keys()) else ''
            fieldFoldersColumns=member['FieldFolders'][keylist[i]]['columns'] if ('FieldFolders' in member.keys()) else ''
            #including fields of DataSetId, Arn, Name, CreatedTime, LastUpdatedTime, PhysicalTableMap_RelationalTable_DataSourceArn, PhysicalTableMap_RelationalTable_Catalog, PhysicalTableMap_RelationalTable_Schema, PhysicalTableMap_RelationalTable_Name, PhysicalTableMap_RelationalTable_InputColumns_Name, PhysicalTableMap_RelationalTable_InputColumns_Type, PhysicalTableMap_CustomSql_DataSourceArn, PhysicalTableMap_CustomSql_Name, PhysicalTableMap_CustomSql_SqlQuery, PhysicalTableMap_CustomSql_Columns_Name, PhysicalTableMap_CustomSql_Columns_Type, PhysicalTableMap_S3Source_DataSourceArn, PhysicalTableMap_S3Source_UploadSettings_Format, PhysicalTableMap_S3Source_UploadSettings_StartFromRow, PhysicalTableMap_S3Source_UploadSettings_ContainsHeader, PhysicalTableMap_S3Source_UploadSettings_TextQualifier, PhysicalTableMap_S3Source_UploadSettings_Delimiter, PhysicalTableMap_S3Source_InputColumns_Name, PhysicalTableMap_S3Source_InputColumns_Type, OutputColumns_Name, OutputColumns_Description, OutputColumns_Type, ImportMode, ConsumedSpiceCapacityInBytes, ColumnGroups_GeoSpatialColumnGroup_Name, ColumnGroups_GeoSpatialColumnGroup_CountryCode, ColumnGroups_GeoSpatialColumnGroup_Columns, FieldFolders_description, FieldFolders_columns
            w = [(member['DataSetId'],member['Arn'],member['Name'],member['CreatedTime'],member['LastUpdatedTime'],
            member['PhysicalTableMap'][keylist[i]]['RelationalTable']['DataSourceArn'] if ('RelationalTable' in member['PhysicalTableMap'][keylist[i]] and 'DataSourceArn' in member['PhysicalTableMap'][keylist[i]]['RelationalTable'].keys()) else "",
            member['PhysicalTableMap'][keylist[i]]['RelationalTable']['Catalog'] if ('RelationalTable' in member['PhysicalTableMap'][keylist[i]] and 'Catalog' in member['PhysicalTableMap'][keylist[i]]['RelationalTable'].keys()) else "",
            member['PhysicalTableMap'][keylist[i]]['RelationalTable']['Schema'] if ('RelationalTable' in member['PhysicalTableMap'][keylist[i]] and 'Schema' in member['PhysicalTableMap'][keylist[i]]['RelationalTable'].keys()) else "",
            member['PhysicalTableMap'][keylist[i]]['RelationalTable']['Name'] if ('RelationalTable' in member['PhysicalTableMap'][keylist[i]] and 'Name' in member['PhysicalTableMap'][keylist[i]]['RelationalTable'].keys()) else "",
            x['Name'] if (x!=None) else '', x['Type'] if (x!=None) else '',
            member['PhysicalTableMap'][keylist[i]]['CustomSql']['DataSourceArn'] if ('CustomSql' in member['PhysicalTableMap'][keylist[i]].keys() and'DataSourceArn' in member['PhysicalTableMap'][keylist[i]]['CustomSql'].keys()) else "",
            member['PhysicalTableMap'][keylist[i]]['CustomSql']['Name'] if ('CustomSql' in member['PhysicalTableMap'][keylist[i]].keys() and 'Name' in member['PhysicalTableMap'][keylist[i]]['CustomSql'].keys()) else "",
            member['PhysicalTableMap'][keylist[i]]['CustomSql']['SqlQuery'] if ('CustomSql' in member['PhysicalTableMap'][keylist[i]].keys() and 'SqlQuery' in member['PhysicalTableMap'][keylist[i]]['CustomSql'].keys()) else "",
            y['Name'] if (y!=None) else '', y['Type'] if (y!=None) else '',
            member['PhysicalTableMap'][keylist[i]]['S3Source']['DataSourceArn'] if ('S3Source' in member['PhysicalTableMap'][keylist[i]].keys() and 'DataSourceArn' in member['PhysicalTableMap'][keylist[i]]['S3Source'].keys()) else "",
            member['PhysicalTableMap'][keylist[i]]['S3Source']['UploadSettings']['Format'] if ('S3Source' in member['PhysicalTableMap'][keylist[i]].keys() and 'Format' in member['PhysicalTableMap'][keylist[i]]['S3Source']['UploadSettings'].keys()) else "",
            member['PhysicalTableMap'][keylist[i]]['S3Source']['UploadSettings']['StartFromRow'] if ('S3Source' in member['PhysicalTableMap'][keylist[i]].keys() and 'StartFromRow' in member['PhysicalTableMap'][keylist[i]]['S3Source']['UploadSettings'].keys()) else "",
            member['PhysicalTableMap'][keylist[i]]['S3Source']['UploadSettings']['ContainsHeader'] if ('S3Source' in member['PhysicalTableMap'][keylist[i]].keys() and 'ContainsHeader' in member['PhysicalTableMap'][keylist[i]]['S3Source']['UploadSettings'].keys()) else False,
            member['PhysicalTableMap'][keylist[i]]['S3Source']['UploadSettings']['TextQualifier'] if ('S3Source' in member['PhysicalTableMap'][keylist[i]].keys() and 'TextQualifier' in member['PhysicalTableMap'][keylist[i]]['S3Source']['UploadSettings'].keys()) else "",
            member['PhysicalTableMap'][keylist[i]]['S3Source']['UploadSettings']['Delimiter'] if ('S3Source' in member['PhysicalTableMap'][keylist[i]].keys() and 'Delimiter' in member['PhysicalTableMap'][keylist[i]]['S3Source']['UploadSettings'].keys()) else "",
            z['Name'] if (z!=None) else '', z['Type'] if (z!=None) else '',
            a['Name'] if (a!=None and 'Name' in a.keys()) else '',
            a['Description'] if (a!=None and 'Description' in a.keys()) else '',
            a['Type'] if (a!=None and 'Type' in a.keys()) else '',
            member['ImportMode'] if ('ImportMode' in member.keys()) else '', 
            member['ConsumedSpiceCapacityInBytes'] if ('ConsumedSpiceCapacityInBytes' in member.keys()) else '',
            b['GeoSpatialColumnGroup']['Name'] if (b!=None and 'Name' in b['GeoSpatialColumnGroup'].keys()) else '',
            b['GeoSpatialColumnGroup']['CountryCode'] if (b!=None and 'CountryCode' in b['GeoSpatialColumnGroup'].keys()) else '',
            b['GeoSpatialColumnGroup']['Columns'] if (b!=None and 'Columns' in b['GeoSpatialColumnGroup'].keys()) else '',
            member['FieldFolders'][keylist[i]]['description'] if ('FieldFolders' in member.keys()) else "",
            member['FieldFolders'][keylist[i]]['columns'] if ('FieldFolders' in member.keys()) else "",
            ) for x,y,z,a,b in itertools.zip_longest(relationalTableInputColumns,customSqlColumns,s3SourceInputColumns,outputColumns,columnGroups)]
            #add data into v_dataset_details
            v_dataset_details.extend(w)
            i+=1
    except:
        pass

    
print("Number of Datasets: " + str(len(v_datasets)))
print("Number of dataset_users: " + str(len(v_dataset_users)))
print("Number of dataset_permissions: " + str(len(v_dataset_permissions)))
print("Number of dataset_details: " + str(len(v_dataset_details)))

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
