import boto3
from sys import argv

if len(argv) < 4:
    print('Usage: consumer.py bucket_1 method bucket_2')
else:
    print('Success')
