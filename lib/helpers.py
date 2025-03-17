from confluent_kafka import Consumer
from confluent_kafka import KafkaError, KafkaException
import socket
import os
from datetime import datetime
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import boto3
from decimal import Decimal

# This function updates the DynamoDB table containing
# analytics information, recording numbers of clicks, visits and signups
# by incrementing these counters from data given in argument.
#
def dynamo_db_update_analytics(table, today, increments):
    # Add for today if doesn't exist yet
    today_item = table.get_item(Key={"Date": today})
    print(' -> Updating analytics item for today in DynamoDB table.')
    if 'Item' not in today_item:
        table.put_item(Item={
            "Date": today,
            "Clicks": 0,
            "Visits": 0,
            "Signups": 0
        })
    
    return table.update_item(
        Key={"Date": today},
        UpdateExpression="set #c = #c + :c, #v = #v + :v, #s = #s + :s",
        ExpressionAttributeNames={
            "#c": "Clicks",
            "#v": "Visits",
            "#s": "Signups"
        },
        ExpressionAttributeValues={
            ":c": increments['clicks'],
            ":v": increments['visits'],
            ":s": increments['signups']
        },
        ReturnValues="UPDATED_NEW",
    )

# This function updates the finance reporting DynamoDB table
# with new finance data given in argument
# (containing order amount, tax, commission and due to merchant values)
#
def dynamo_db_update_finance_reporting(table, today, finance_data):
    # Add for today if doesn't exist yet
    today_item = table.get_item(Key={"Date": today})
    
    print(' -> Updating finance reporting item for today in DynamoDB table.')
    if 'Item' not in today_item:
        table.put_item(Item={
            "Date": today,
            "TotalAmount": Decimal(0.0),
            "TotalItemCount": 0,
            "TotalTax": Decimal(0.0),
            "TotalCommission": Decimal(0.0),
            "TotalDueToMerchant": Decimal(0.0)
        })
    
    return table.update_item(
        Key={"Date": today},
        UpdateExpression="set #ta = #ta + :ta, #c = #c + :c, #tt = #tt + :tt, #tc = #tc + :tc, #td = #td + :td",
        ExpressionAttributeNames={
            "#ta": "TotalAmount",
            "#c": "TotalItemCount",
            "#tt": "TotalTax",
            "#tc": "TotalCommission",
            "#td": "TotalDueToMerchant"
        },
        ExpressionAttributeValues={
            ":ta": Decimal(finance_data['amount']),
            ":c": 1,
            ":tt": Decimal(finance_data['tax']),
            ":tc": Decimal(finance_data['commission']),
            ":td": Decimal(finance_data['due_to_merchant'])
        },
        ReturnValues="UPDATED_NEW",
    )

# This function updates the fraud detection DynamoDB table
# with new data given in argument
#
def dynamo_db_update_fraud_detection(table, fraud_data):
    # Add for today if doesn't exist yet
    user_id = fraud_data['user_id']
    user_item = table.get_item(Key={"UserId": user_id})
    
    print(' -> Updating fraud detection item for user in DynamoDB table.')
    if 'Item' not in user_item:
        table.put_item(Item={
            "UserId": user_id,
            "TotalOrderFlagged": 0
        })
    
    return table.update_item(
        Key={"UserId": user_id},
        UpdateExpression="set #t = #t + :t",
        ExpressionAttributeNames={
            "#t": 'TotalOrderFlagged'
        },
        ExpressionAttributeValues={
            ":t": 1,
        },
        ReturnValues="UPDATED_NEW",
    )

# Utilily function to create a Kafka consumer.
def make_consumer(bootstrap_server_url):
    def oauth_cb(oauth_config):
        auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token("eu-west-2")
        # Note that this library expects oauth_cb to return expiry time in seconds since epoch, while the token generator returns expiry in ms
        return auth_token, expiry_ms/1000

    conf = {'bootstrap.servers': bootstrap_server_url,
            'group.id': socket.gethostname(),
            'enable.auto.commit': 'false',
            'auto.offset.reset': 'latest',
            'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'OAUTHBEARER',
        'oauth_cb': oauth_cb,}

    return Consumer(conf)

# Utility function for an infinite loop to poll for messages.
def basic_consume_loop(consumer, topics, process_msg_function):
    running = True
    try:
        consumer.subscribe(topics)

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                process_msg_function(msg)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()