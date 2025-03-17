from decimal import Decimal
import pytest
import socket
import json
import time
from confluent_kafka import Producer
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import os
import uuid
from dotenv import load_dotenv
import boto3
from datetime import datetime

load_dotenv()

def oauth_cb(oauth_config):
    auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token("eu-west-2")
    # Note that this library expects oauth_cb to return expiry time in seconds since epoch, while the token generator returns expiry in ms
    return auth_token, expiry_ms/1000

conf = {
    # Make sure to use the public bootstrap URL of your cluster here
    'bootstrap.servers': 'b-2.watermonitoringeventsm.k7yy6p.c3.kafka.eu-west-2.amazonaws.com:9098,b-1.watermonitoringeventsm.k7yy6p.c3.kafka.eu-west-2.amazonaws.com:9098',
    'client.id': socket.gethostname(),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'OAUTHBEARER',
    'oauth_cb': oauth_cb,
}

producer = Producer(conf)

class TestDataPipeline:
    
    # Make sure to run your test after having set 
    # AWS access keys environment variables in your environment
    # (you can use the .dotenv library to do this).
    # Otherwise boto will fail to find the 'resource' (the DynamoDB table).
    
    def test_data_pipeline_success_for_finance_reporting(self):
        dynamodb_resource = boto3.resource("dynamodb", region_name='eu-west-2')
        table = dynamodb_resource.Table("water-finance-reporting")
        today = datetime.today().strftime('%Y-%m-%d')


        
        # Get initial total amount for today
        item_data = table.get_item(Key={'Date': today})

        current_total_item_count = item_data['Item']['TotalItemCount']
        
        # 1. Send JSON input event to topic
        # json = '{"user_id":"'+str(current_user_id)+'", "event_name": "'+event+'", "page": "'+path+'", "item_url": '+item+', "order_email": "'+email+'", "amount": "'+str(amount)+'" }'
        json_str = json.dumps({
            'user_id': 'fake-user-id',
            'event_name': 'order_confirmed',
            'page': '/test-page',
            'item_url': 'my-fake-product',
            'amount': 10.00
        })
        
        producer.produce('events', key="order-"+str(uuid.uuid4()), value=bytes(json_str, encoding="utf-8"))
        producer.flush(3)
        
        # 2. Wait some given time.
        time.sleep(3)
        
        # 3. Assert resulting analytical data
        new_item_data = table.get_item(Key={'Date': today})
        new_total_item_count = new_item_data['Item']['TotalItemCount']

        assert current_total_item_count < new_total_item_count

    # {'Item': {'TotalItemCount': Decimal('43827'), 'Date': '2025-03-17', 'TotalDueToMerchant': Decimal('1481597.9599999999751100027145999'), 'TotalAmount': Decimal('1949471'), 'TotalCommission': Decimal('77978.840000000003246521385043685'), 'TotalTax': Decimal('389894.20000000002164347590041119')}, 'ResponseMetadata': {'RequestId': 'F77AENV21Q2DIPQUJ7ND65JL2JVV4KQNSO5AEMVJF66Q9ASUAAJG', 'HTTPStatusCode': 200, 'HTTPHeaders': {'server': 'Server', 'date': 'Mon, 17 Mar 2025 15:37:23 GMT', 'content-type': 'application/x-amz-json-1.0', 'content-length': '273', 'connection': 'keep-alive', 'x-amzn-requestid': 'F77AENV21Q2DIPQUJ7ND65JL2JVV4KQNSO5AEMVJF66Q9ASUAAJG', 'x-amz-crc32': '514175792'}, 'RetryAttempts': 0}}