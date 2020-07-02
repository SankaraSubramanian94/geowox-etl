# Install and Import boto3 library using pip package for accessing s3 buckets

#pip install boto3
import boto3, os
s3 = boto3.resource('s3')

# Check the website is working fine and load page content into a variable
import requests, sys
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
#url = "https://www.propertypriceregister.ie/"
import os, ssl
if (not os.environ.get('PYTHONHTTPSVERIFY', '') and getattr(ssl, '_create_unverified_context', None)):
    ssl._create_default_https_context = ssl._create_unverified_context

url = sys.argv[1]
print(url)
req = Request(url)
try:
    response = urlopen(req)
except HTTPError as e:
    print('The server couldn\'t fulfill the request.')
    print('Error code: ', e.code)
    sys.exit(1)
except URLError as e:
    print('We failed to reach a server.')
    page = requests.get(url, verify=False)
    print(page.status_code)
    
# Create a directory and 777 permission is given
import os
from bs4 import BeautifulSoup
cwd = os.getcwd()
newdir = cwd +"\\extract"
print ("The current Working directory is " + cwd)
os.makedirs( newdir, 0o777, exist_ok=True);

# A text file is created and all the zip file url is stored in the file
print ("Created new directory " + newdir)
newfile = open('zipfiles.txt','w')
print (newfile)

#File extension to be looked for. 
extension = ".zip"

#Use BeautifulSoup to clean up the page
soup = BeautifulSoup(page.content)
soup.prettify()

#Find all the links on the page that end in .zip and store it in the text file
for anchor in soup.findAll('a', href=True):
    links = url + anchor['href']
    if links.endswith(extension):
        newfile.write(links + '\n')
newfile.close()

#Read what is saved in zipfiles.txt and output it to the user
#This is done to create presistent data 
newfile = open('zipfiles.txt', 'r')
for line in newfile:
    print (line, sep='\n')
newfile.close()

#Read through the lines in the text file and download the zip files.
#Handle exceptions and print exceptions to the console
with open('zipfiles.txt', 'r') as url:
    for line in url:
        if line:
            try:
                ziplink = line.rstrip('\n') # strip tailing newline character
                # Extracting the zip file name from the URL
                firstpos=ziplink.rfind("/")
                lastpos=len(ziplink)
                zipfile = ziplink[firstpos+1:lastpos]
                print("Zip file name " + zipfile)
                print ("Trying to reach " + ziplink)
                response = urlopen(ziplink)
            except URLError as e:
                if hasattr(e, 'reason'):
                    print ('We failed to reach a server.')
                    print ('Reason: ', e.reason)
                    continue
                elif hasattr(e, 'code'):
                    print ('The server couldn\'t fulfill the request.')
                    print ('Error code: ', e.code)
                    continue
            else:
                zipcontent = response.read()
                completeName = os.path.join(newdir, zipfile)
                with open (completeName, 'wb') as f:
                    print ("downloading.. " + zipfile)
                    f.write(zipcontent)
                    f.close()
print ("Script completed")


from botocore.exceptions import ClientError
import logging

# Upload the file extracted from webpage to s3 bucket in extract folder
s3 = boto3.resource('s3')
BUCKET = "geowox-data-engineer-role"
try:
    print('Bucket_name:' + BUCKET)    
    s3.Bucket(BUCKET).upload_file(completeName, "extract/zip/" + zipfile)
    print('File uploaded to extract folder successfully!')

except ClientError as e:
    logging.error(e)
    print('File Upload Failed!!')
