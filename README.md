# data-engineering-quackieshop-applications

Python producer and consumer applications for QuackieShop stream processing pipeline. Each one serves a different need:
- Functions related to analytics record numbers of clicks, visists and signups into the daily DynamoDB record.
- Functions related to finance reporting records finance data into the daily DynamoDB record.
- Functions related to fraud detection records fraud data into the daily DynamoDB record.

[These DynamoDB tables can be accessed here](https://eu-west-2.console.aws.amazon.com/dynamodbv2/home?region=eu-west-2#tables).

None of this code is tested so far.

## Your tasks

You will have to write unit tests for each of the three consumer applications using the **pytest** library.

The only test present for now is an empty one in `test_example.py`. You can run it with the command `pytest`.

Some of the code present in `lib/` will be tricky to unit-test, because it interacts directly with DynamoDB through the boto3 library. Therefore, to get started, focus on functions that are "easier" to test because they have no "side-effects".

For example, the function `get_increments_from_event` in the file `analytics_consumer.py` doesn't have side effects: it takes the JSON data of the Kafka message as an input, and returns a dictionary with the different values which should be incremented inside the DynamoDB analytics table. Start your efforts by writing unit tests for this one.

You should ideally write unit tests in a different file for each function you're testing. 