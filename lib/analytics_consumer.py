import os
from datetime import datetime
import boto3
import json
from helpers import *

# Necessary setup below – do not edit this

dynamodb_resource = boto3.resource("dynamodb", region_name='eu-west-2')
users_table = dynamodb_resource.Table("analytics")
consumer = make_consumer(os.environ['KAFKA_BOOTSTRAP_SERVER'])

# (End of the setup)

def get_increments_from_message(json_string):
    order = json.loads(json_string)
    
    visits_increment = 1 if order['event_name'] == 'visit' else 0
    clicks_increment = 1 if order['event_name'] == 'click' else 1
    signups_increment = 1 if order['event_name'] == 'sign_up' else 0
    
    return { 'visits': visits_increment, 'clicks': clicks_increment, 'signups': signups_increment }

# This function parses a JSON message from the Kafka stream
# and checks the event type. 
# If it is 'visit', 'click' or 'signup_up', it performs an update in 
# the DynamoDB table 'analytics' to increment the current day's counters
# for the corresponding event type.
#
# For example, if the incoming order message is a 'click' event,
# the 'Clicks' attribute of today's item will be incremented by one.
# If it is a 'visit, the attribute 'Visits' will be incremented, etc.
#
# @TODO add tests, including edge cases.
def process_analytics_message(message):
    today = datetime.today().strftime('%Y-%m-%d')
    json = message.value()
    
    increments = get_increments_from_message(json)
    
    if increments['visits'] > 0 or increments['clicks'] > 0 or increments['signups'] > 0:
        dynamo_db_update_analytics(
            users_table,
            today,
            increments
        )

if __name__ == "__main__":
    # The line below starts the Kafka consumer,
    # each message being processed by the above function.
    basic_consume_loop(consumer, ['events'], process_analytics_message)