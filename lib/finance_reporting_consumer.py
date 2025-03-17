import os
from datetime import datetime
import boto3
import json
from helpers import *

# Necessary setup below – do not edit this

dynamodb_resource = boto3.resource("dynamodb", region_name='eu-west-2')
finance_table = dynamodb_resource.Table("finance-reporting")
consumer = make_consumer(os.environ['KAFKA_BOOTSTRAP_SERVER'])

# (End of the setup)

# The platform gets a 5% commission on all order purchases (after tax)
# and the rest goes to the merchant.
#
# For example:
#   if the amount is 20
#   then tax should be 4
#        amount without tax is then 16
#        commission should be 5% of 16 = 0.8
#        and due to merchant should then be 15.2
#
def get_finance_reporting_data_from_message(json_string):
    order = json.loads(json_string)
    amount = Decimal(0.0)
    
    if order['event_name'] == 'order_confirmed':
        amount = Decimal(order['amount'])
        
    tax = amount * Decimal(0.2)
    
    commission_value = (amount - tax) * Decimal(0.05)
    due_to_merchant_value = amount - tax - commission_value

    return {
        'amount': amount,
        'count': 1,
        'tax': tax,
        'commission': commission_value,
        'due_to_merchant': due_to_merchant_value
    }

# This function parses a JSON message from the Kafka stream
# and considers only 'order' message types, to update the current 
# total amount £ from order sales for today's date.
#
# @TODO add tests, including edge cases.
def process_analytics_message(message):
    order = json.loads(message.value())
    json_string = message.value()
    finance_data = get_finance_reporting_data_from_message(json_string)
    today = datetime.today().strftime('%Y-%m-%d')
    
    if 'amount' in order:
        dynamo_db_update_finance_reporting(
            finance_table,
            today,
            finance_data
        )
        return 

if __name__ == "__main__":
    # The line below starts the Kafka consumer,
    # each message being processed by the above function.
    basic_consume_loop(consumer, ['events'], process_analytics_message)