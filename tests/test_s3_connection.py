python -c "
import boto3, os
from dotenv import load_dotenv
load_dotenv()
s3 = boto3.client('s3')
buckets = s3.list_buckets()
print('Connected! Buckets:', [b['Name'] for b in buckets['Buckets']])
"