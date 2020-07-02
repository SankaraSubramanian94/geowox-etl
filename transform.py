# Extract zip file from s3 and unzip and store it back in csv format
import boto3
from io import BytesIO
import zipfile

bucket = 'geowox-data-engineer-role' #bucket name

s3 = boto3.client('s3', use_ssl=False)
prefix = 'extract/zip/'

zipped_keys =  s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter = "/")

file_list = []
for key in zipped_keys['Contents']:
    file_list.append(key['Key'])
    
#This will give you list of files in the folder you mentioned as prefix
s3_resource = boto3.resource('s3')

#Now create zip object one by one, this below is for 1st file in file_list
zip_obj = s3_resource.Object(bucket_name=bucket, key=file_list[0])
print (zip_obj)
buffer = BytesIO(zip_obj.get()["Body"].read())

# store file in s3 bucket in csv format
z = zipfile.ZipFile(buffer)
for filename in z.namelist():
    file_info = z.getinfo(filename)
    s3_resource.meta.client.upload_fileobj(
        z.open(filename),
        Bucket=bucket,
        #Key='extract/csv/' + f'{filename}')
        Key='extract/csv/' + 'ppr.csv')
    print("Zip file extracted and stored in csv format")
    
# Read data from s3 and load into a pandas dataframe
import pandas as pd
import boto3
from io import StringIO

file_name = 'extract/csv/ppr.csv'
bucket = 'geowox-data-engineer-role'

s3_resource = boto3.resource('s3')

s3_object = s3_resource.Object(bucket_name=bucket, key=file_name)

s3_data = StringIO(s3_object.get()['Body'].read().decode('windows-1252'))
data = pd.read_csv(s3_data)

# Generate ID column and indexing ID to identify unique records
data.insert(0, 'id', range(1, 1 + len(data)))
data.index.name = 'id'
data = data.reset_index(drop=True)

# Column Rename according to the standards
data.rename(columns ={
'Date of Sale (dd/mm/yyyy)':'sales_date',
'Address': 'address',
'Postal Code': 'postal_code',
'County': 'county',
'Price (€)': 'sales_value',
'Not Full Market Price': 'not_full_market_price_ind',
'VAT Exclusive': 'vat_exclusive_ind',
'Description of Property': 'new_home_ind',
'Property Size Description' : 'property_size_description'
},
inplace = True)

# Data Transformation: 1) create column month_start from sales_data column
data['month_start'] = '01/' + (data.sales_date.astype(str).str[3:])

# Data Transformation: 2) Remove special characters and lowercase the string
data['address'] = data['address'].str.replace('-','').str.lower()

# Data Transformation: 3) Lowercase the string
data['county'] = data['county'].str.lower()

# Data Transformation: 4) Strip "£" symbol, remove "," and convert number to float
data['sales_value'] = pd.to_numeric(data['sales_value'].str[1:].str.replace(',',''), errors='coerce').astype(float)

# Data Transformation: 5) Generate a column "not_full_market_price_ind" and "vat_exclusive_ind" based on Yes:1/No:0
data['not_full_market_price_ind'] = [0 if x == 'No' else 1 for x in data['not_full_market_price_ind']]
data['vat_exclusive_ind'] = [0 if x == 'No' else 1 for x in data['vat_exclusive_ind']]

# Data Transformation: 6) Generate a column "new_home_ind" based on the description of property
data['new_home_ind'] = [1 if x == 'New Dwelling house /Apartment' else 0 for x in data['new_home_ind']]

# Create list of counties in Ireland
county_list = ['Cork', 'Galway', 'Mayo', 'Donegal', 'Kerry', 'Tipperary', 'Clare', 'Tyrone', 'Antrim', 'Limerick', 'Roscommon', 'Down', 'Wexford', 'Meath', 'Londonderry', 'Kilkenny', 'Wicklow', 'Offaly', 'Cavan', 'Waterford', 'Westmeath', 'Sligo', 'Laois', 'Kildare', 'Fermanagh', 'Leitrim', 'Armagh', 'Monaghan', 'Longford', 'Dublin', 'Carlow', 'Louth']
county_list = [element.lower() for element in county_list]

# Remove duplicate records based on address, county and sales_value
non_duplicate_records = data.drop_duplicates(subset = ["address","county","sales_value"])

# Duplicate records in the dataframe
duplicate_records = data[~data.isin(non_duplicate_records)]

# Data Transformation: 7) Generate a column "quarantine_ind" based on the unique values and valid counties in Ireland
import numpy as np
data['quarantine_ind'] = np.where(data['address'].isin(duplicate_records['address']),1,(np.where(~data['county'].isin(county_list),1,0)))

# Data Transformation: 8) Generate a column "quarantine_code" that state the reason for quarantine based on the quarantine_ind
data['quarantine_code'] = ["Address, County and Sales values are duplicated" if x == 1 else "" for x in data['quarantine_ind']]

# Transformed Dataset
data = data[['id','sales_date','month_start','address','county', 'sales_value', 'not_full_market_price_ind', 'vat_exclusive_ind', 'new_home_ind', 'quarantine_ind', 'quarantine_code' ]].reset_index(drop=True)

# Load data from pandas dataframe to s3 bucket in transform folder
from io import StringIO
import boto3
from botocore.exceptions import ClientError
import logging

bucket = 'geowox-data-engineer-role'

try:
    print('Bucket_name:' + bucket)
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, 'transform/ppr.csv').put(Body=csv_buffer.getvalue())
    print('Transformed data stored in transform folder')

except ClientError as e:
    logging.error(e)
    print('File Upload Failed!!')
