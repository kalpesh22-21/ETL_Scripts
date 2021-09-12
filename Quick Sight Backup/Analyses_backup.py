import json
import boto3
import re
client = boto3.client('quicksight')

# ARN of datasoucre on which the datasets are created
Source_Account = '399183144478'
Is_End = False
analyses_ids = []
analyses_names = []
cannot_be_backed = []

analyses = client.list_analyses(
                AwsAccountId= Source_Account,
                MaxResults=100,
                )

for member in analyses['AnalysisSummaryList']:
    analyses_ids.append(member['AnalysisId'])
    analyses_names.append(member['Name'])


while Is_End == False :
    analyses = client.list_analyses(
                AwsAccountId= Source_Account,
                MaxResults=100,
                NextToken=analyses['NextToken']
                )
    
    for member in analyses['AnalysisSummaryList']:
        analyses_ids.append(member['AnalysisId'])
        analyses_names.append(member['Name'])


print(len(analyses_ids))

for i in range(len(analyses_ids):
    try:
        analysis_details = client.describe_analysis(
            AwsAccountId = Source_Account,
            AnalysisId = analyses_ids[i]
            )
        dataset_arns  = analysis_details['Analysis']['DataSetArns']
        dataset_ = []
        for j in range(len(dataset_arns)):
             dataset_.append({'DataSetPlaceholder': analyses_names[i] + ' dataset ' + str(j) , 'DataSetArn': dataset_arns[j]})
            
        template_response = client.create_template(
             AwsAccountId = Source_Account,
             TemplateId = analyses_ids[i] + '_analysis_template',
             Name = analyses_names[i] + '_analysis_template' ,
             SourceEntity = { 'SourceAnalysis': { 'Arn' : analysis_details['Analysis']['Arn'] , 'DataSetReferences': dataset_ }},
            )
        print('The Template with name {0} is created with analyses_id = {1}'.format(analyses_names[i] + '_analysis_template',template_response["TemplateId"]))
        
        dashboard_response = client.create_dashboard(
             AwsAccountId = Source_Account,
             DashboardId = analyses_ids[i] + '_backed_analysis' ,
             Name = analyses_names[i] + '_backed_analysis'  ,
             SourceEntity = { 'SourceTemplate': { 'Arn' : template_response['Arn'] , 'DataSetReferences': dataset_ }},
            )
        
        print('The Dashboard with name {0} is created with analyses_id = {1}'.format(analyses_names[i] + '_backed_analysis',template_response["TemplateId"]))
    
    except Exception as e:
        print(e)
        print("The Analysis {0} cannot be backed up".format(analyses_ids[i]))
        cannot_be_backed.append(analyses_ids[i])
