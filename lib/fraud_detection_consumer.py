import os
from datetime import datetime
import boto3
import json
import time
from helpers import *

# Necessary setup below – do not edit this

dynamodb_resource = boto3.resource("dynamodb", region_name='eu-west-2')
fraud_table = dynamodb_resource.Table("fraud-detection")
consumer = make_consumer(os.environ['KAFKA_BOOTSTRAP_SERVER'])

# Records a dictionary of user_id -> timestamp of last purchase
purchase_counters = {}

# (End of the setup)

# Returns fraud detection data from incoming message.
# Potential fraud is detected if two order_confirmed events 
# are detected with less than 5 seconds between them.
def get_fraud_detection_data_from_message(json_string):
    order = json.loads(json_string)
    user_id = order['user_id']
    order_time_ts = order['order_time']
    flag_for_fraud = False
    
    # user if 
    if user_id not in purchase_counters:
        purchase_counters[user_id] = int(round(time.time()))
    else:
        last_purchase_ts_for_user = purchase_counters[user_id]
        if int(order_time_ts) - int(last_purchase_ts_for_user) < 5:
            flag_for_fraud = True
    
    return {
        'user_id': user_id,
        'order_time': order_time_ts,
        'flag_for_fraud': flag_for_fraud
    }
        

# This function parses a JSON message from the Kafka stream
# and...
#
# @TODO add tests, including edge cases.
def process_analytics_message(message):
    json_string = message.value()
    
    fraud_result = get_fraud_detection_data_from_message(json_string)
    if fraud_result['flag_for_fraud'] == True:
        print('Flagged for fraud! Recording in DynamoDB')
        dynamo_db_update_fraud_detection(
            fraud_table,
            fraud_result
        )
    
    return

if __name__ == "__main__":
    # The line below starts the Kafka consumer,
    # each message being processed by the above function.
    basic_consume_loop(consumer, ['events'], process_analytics_message)