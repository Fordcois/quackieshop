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
    'bootstrap.servers': 'b-2-public.watermonitoringeventsm.k7yy6p.c3.kafka.eu-west-2.amazonaws.com:9198,b-1-public.watermonitoringeventsm.k7yy6p.c3.kafka.eu-west-2.amazonaws.com:9198',
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

        assert new_total_item_count  == (current_total_item_count +1 )
