# Read Transformed data from s3 and load into a pandas dataframe
import pandas as pd
import boto3
from io import StringIO

file_name = 'transform/ppr.csv'
bucket = 'geowox-data-engineer-role'

s3_resource = boto3.resource('s3')

s3_object = s3_resource.Object(bucket_name=bucket, key=file_name)

s3_data = StringIO(s3_object.get()['Body'].read().decode('utf-8'))
transform_data = pd.read_csv(s3_data)

# Backup of the exisiting file to history folder that avoids data loss
import boto3
import time
s3 = boto3.resource('s3')
copy_source = {
    'Bucket': 'geowox-data-engineer-role',
    'Key': 'load/ppr_current.csv'
}
timestr = time.strftime("%Y%m%d-%H%M%S")
print (timestr)
s3.meta.client.copy(copy_source, 'geowox-data-engineer-role', 'load/history/'+ timestr + '/ppr.csv')

# Read current data from s3 and load into a pandas dataframe
import pandas as pd
import boto3
from io import StringIO

file_name = 'load/ppr_current.csv'
bucket = 'geowox-data-engineer-role'

s3_resource = boto3.resource('s3')

s3_object = s3_resource.Object(bucket_name=bucket, key=file_name)

s3_data = StringIO(s3_object.get()['Body'].read().decode('utf-8'))
current_data = pd.read_csv(s3_data)

# Data Transformation of the current file to match the transformed data structure
current_data['month_start'] = current_data['month_start'].replace('-','/', regex=True)

# Select unique records from current data 
unique_records_current = current_data.drop_duplicates(subset = ["address","county","sales_value"])

# Duplicate records of current data are stored in
duplicate_records_current = current_data[~current_data.id.isin(unique_records_current.id)]

# Select unique records from transformed data 
unique_records_transform = transform_data.drop_duplicates(subset = ["address","county","sales_value"])

# Duplicate records of transformed data are stored in
duplicate_records_transform = transform_data[~transform_data.id.isin(unique_records_transform.id)]


# Final data that union distinct records from current and transformed data based on "address","county","sales_value" attributes
combined_data = pd.concat([unique_records_current, unique_records_transform]).drop_duplicates(subset=["address","county","sales_value"]).sort_values(by=['id']).sort_values(by=['id']).reset_index(drop=True)

# Delete ID and reset Index
del combined_data['id']
combined_data.insert(0, 'id', range(1, 1 + len(combined_data)))
combined_data.index.name = 'id'
combined_data =combined_data.reset_index(drop=True)


# Load data from pandas dataframe to s3 bucket in load folder
from io import StringIO
import boto3
from botocore.exceptions import ClientError
import logging

bucket = 'geowox-data-engineer-role'

try:
    print('Bucket_name:' + bucket)
    csv_buffer = StringIO()
    combined_data.to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, 'load/ppr_current.csv').put(Body=csv_buffer.getvalue())
    print('Finally historical + current data are stored in load folder')

except ClientError as e:
    logging.error(e)
    print('File Upload Failed!!')
