import json
import boto3
import re
client = boto3.client('quicksight')

# ARN of datasoucre on which the datasets are created
Source_Account = '399183144478'
Is_End = False
dataset_ids = []
dataset_names = []
cannot_be_backed = []

data_sets = client.list_data_sets(
                AwsAccountId= Source_Account,
                MaxResults=100,
                )

for member in data_sets["DataSetSummaries"]:
    dataset_ids.append(member['DataSetId'])
    dataset_names.append(member['Name'])


while 'NextToken' in data_sets:
    data_sets = client.list_data_sets(
                AwsAccountId= Source_Account,
                MaxResults=100,
                NextToken=data_sets['NextToken']
                )
    
    for member in data_sets["DataSetSummaries"]:
        dataset_ids.append(member['DataSetId'])
        dataset_names.append(member['Name'])
        


print(len(dataset_ids))

for i in range(len(dataset_ids)):
    try:
        source_dataset_details = client.describe_data_set(
            AwsAccountId=Source_Account,
            DataSetId=dataset_ids[i]
            )
        # print(source_dataset_details)
        
        # Physical Table Map
        try:
            physicalmap = source_dataset_details["DataSet"]['PhysicalTableMap']
            # print(physicalmap)
        except:
            print('No Physical Table Map')
            physicalmap = ''
        
        # Logical Table Map
        try:
            keys = source_dataset_details["DataSet"]['LogicalTableMap'].keys()
            logicalmap = {}
            for key in keys:
                logicalmap[key] = source_dataset_details["DataSet"]['LogicalTableMap'][key]
            # print(logicalmap)
        except:
            print('No Logical Table Map')
            logicalmap = {}
        
            
        response = client.create_data_set(
             AwsAccountId = Source_Account,
             DataSetId = dataset_ids[i] + '_backup',
             Name = dataset_names[i] + '_backup' ,
             PhysicalTableMap = physicalmap,
             LogicalTableMap = logicalmap,
             ImportMode=source_dataset_details["DataSet"]['ImportMode'],
            )
        print('The dataset with name {0} is created in DB2 with dataset_id = {1}'.format(dataset_names[i]+'_backup',response["DataSetId"]))
    
    except Exception as e:
        print(e)
        print("The dataset {0} cannot be backed up".format(dataset_ids[i]))
        cannot_be_backed.append(dataset_ids[i])