import boto3
import json
from datetime import datetime
import time

import os
aws_key=os.environ['aws_key']
aws_secret_key=os.environ['aws_secret_key']
my_stream_name = 'test-kinesis'

kinesis_client = boto3.client('kinesis', region_name='us-east-1',aws_access_key_id=aws_key,
    aws_secret_access_key=aws_secret_key)


response = kinesis_client.describe_stream(StreamName=my_stream_name)

my_shard_id = response['StreamDescription']['Shards'][0]['ShardId']

shard_iterator = kinesis_client.get_shard_iterator(StreamName=my_stream_name,
                                                      ShardId=my_shard_id,
                                                      ShardIteratorType='LATEST')

my_shard_iterator = shard_iterator['ShardIterator']

record_response = kinesis_client.get_records(ShardIterator=my_shard_iterator,
                                              Limit=2)

while 'NextShardIterator' in record_response:
    record_response = kinesis_client.get_records(ShardIterator=record_response['NextShardIterator'],
                                                  Limit=2)

    print (record_response)

    # wait for 5 seconds
    time.sleep(5)
