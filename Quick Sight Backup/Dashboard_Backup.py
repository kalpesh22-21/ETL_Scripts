import json
import boto3
import re
client = boto3.client('quicksight')

# ARN of datasoucre on which the datasets are created
Source_Account = ''
Is_End = False
dashboard_ids = []
dashboard_names = []
cannot_be_backed = []

dashboards = client.list_dashboards(
                AwsAccountId= Source_Account,
                MaxResults=100,
                )

for member in dashboards['DashboardSummaryList']:
    dashboard_ids.append(member['DashboardId'])
    dashboard_names.append(member['Name'])


 while Is_End == False :
     dashboards = client.list_dashboards(
                 AwsAccountId= Source_Account,
                 MaxResults=100,
                 )
     for member in dashboards['DashboardSummaryList']:
         dashboard_ids.append(member['DashboardId'])
         dashboard_names.append(member['Name'])



print(len(dashboard_ids))

for i in range(len(dashboard_ids)):
    try:
        dashboard_details = client.describe_dashboard(
            AwsAccountId = Source_Account,
            DashboardId = dashboard_ids[i]
            )
            
        # print(dashboard_details)
        source_arn  = dashboard_details['Dashboard']['Version']['SourceEntityArn']
        dataset_arns = dashboard_details['Dashboard']['Version']['DataSetArns']
        
        print(source_arn)
        if source_arn.split(':')[5].split('/')[0] == 'analysis':
            dataset_ = []
            for j in range(len(dataset_arns)):
                dataset_.append({'DataSetPlaceholder': dashboard_names[i] + ' dataset ' + str(j) , 'DataSetArn': dataset_arns[j]})
            
            print(dataset_)
            
            template_response = client.create_template(
                 AwsAccountId = Source_Account,
                 TemplateId = dashboard_ids[i] + '_dashboard_template',
                 Name = dashboard_names[i] + '_dashboard_template' ,
                 SourceEntity = { 'SourceAnalysis': { 'Arn' : source_arn , 'DataSetReferences': dataset_ }},
                )
            print('The Template with name {0} is created with analyses_id = {1}'.format(dashboard_names[i] + '_dashboard_template',template_response["TemplateId"]))
        
            dashboard_response = client.create_dashboard(
                 AwsAccountId = Source_Account,
                 DashboardId = dashboard_ids[i] + '_backed_dashboard',
                 Name = dashboard_names[i] + '_backed_dashboard' ,
                 SourceEntity = { 'SourceTemplate': { 'Arn' : template_response['Arn'] , 'DataSetReferences': dataset_ }},
                )
            
            print('The Dashboard with name {0} is created with analyses_id = {1}'.format(dashboard_names[i] + '_backed_dashboard',template_response["TemplateId"]))
    
    except Exception as e:
        print(e)
        print("The Analysis {0} cannot be backed up".format(dashboard_ids[i]))
        cannot_be_backed.append(dashboard_ids[i])
