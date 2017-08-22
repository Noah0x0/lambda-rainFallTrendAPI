import json
import urllib.parse
import boto3

client = boto3.client('s3')

bucket = 'test-uodu-s3'
key = '/japan/ishikawa/asanogawa/rain_fall_trend.json'

def lambda_handler(event, context):
    try:
        response = client.get_object(Bucket=bucket, Key=key)
        body = response['Body'].read().decode('utf-8')
        result = json.loads(body)
        return result
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
