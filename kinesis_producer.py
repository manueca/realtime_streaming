import boto3
import json
from datetime import datetime
import calendar
import random
import time
import os
aws_key=os.environ['aws_key']
aws_secret_key=os.environ['aws_secret_key']
my_stream_name = 'test-kinesis'

kinesis_client = boto3.client('kinesis', region_name='us-east-1',aws_access_key_id=aws_key,
    aws_secret_access_key=aws_secret_key)

def put_to_stream(thing_id, property_value, property_timestamp):
    payload = {
                'prop': str(property_value),
                'timestamp': str(property_timestamp),
                'thing_id': thing_id
              }


    put_response = kinesis_client.put_record(
                        StreamName=my_stream_name,
                        Data=json.dumps(payload),
                        PartitionKey=thing_id)

while True:
    property_value = random.randint(40, 120)
    property_timestamp = calendar.timegm(datetime.utcnow().timetuple())
    thing_id = 'jerry-test'

    put_to_stream(thing_id, property_value, property_timestamp)
    print (thing_id,property_value,property_timestamp)
    # wait for 5 second
    time.sleep(2)
