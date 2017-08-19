import json
import urllib.parse
import boto3

client = boto3.client('s3')

bucket = 'test-uodu-s3'
target = 'trend'

def get_rainfall_trend(key):
    response = client.get_object(Bucket=bucket, Key=key)
    body = response['Body'].read().decode('utf-8')
    return body

def set_response_body(status_code, body):
    headers = {}
    headers['Content-Type'] = 'application/json'
    
    res_body = {}
    res_body['statusCode'] = status_code
    res_body['headers'] = headers
    res_body['body'] = body

    return res_body

def lambda_handler(event, context):

    # クエリが渡されてない場合$
    if (event['pathParameters'] is None):
        return set_response_body(400, 'Bad Request No PathParameters')
    else:
        params = event['pathParameters']
        
    # クエリパラメータが不正な場合のデフォルトを荒川に
    if (set(params) >= {'country', 'prefectures', 'river'}):
        country = params['country']
        prefectures = params['prefectures']
        river = params['river']
        key = target + '/' + country + '/' + prefectures + '/' + river + '/trendRainFall.json'
    else:
        return set_response_body(400, 'Bad Request No Item')
    
    try:
        json_str = get_rainfall_trend(key)
        
        return set_response_body(200, json_str)
    except Exception as e:
        print(e)
        return set_response_body(500, 'File Not Found')
