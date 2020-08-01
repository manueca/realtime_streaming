from __future__ import print_function
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream
import sys,os
from pyspark import SparkContext
from pyspark.streaming import StreamingContext



def f(iterator):
	for x in iterator:
		print(x)


def printRecord(rdd):
    print("========================================================")
    print("Starting new RDD")
    print("========================================================")
    rdd.foreach(f)

if __name__ == "__main__":
    reload(sys)  
    sys.setdefaultencoding('utf-8')
    sc = SparkContext(appName="PythonStreamingKinesisWordCountAsl")
    ssc = StreamingContext(sc, 10)
    AWS_ACCESS_KEY=os.environ['aws_key']
    AWS_SECRET_KEY=os.environ['aws_secret_key']
    sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", AWS_ACCESS_KEY)
    sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", AWS_SECRET_KEY)
    endpointUrl='arn:aws:kinesis:us-east-1:022697672595:stream/test-kinesis'
    streamName='test-kinesis'
    appName='Kinesis-Streaming'
    regionName='us-east-1'
    #appName, streamName, endpointUrl, regionName = sys.argv[1:]
    regionName='us-east-1'
    dstream = KinesisUtils.createStream(
        ssc, appName, streamName, endpointUrl, regionName, InitialPositionInStream.TRIM_HORIZON, 10)
    dstream.foreachRDD(printRecord)
    ssc.start()
    ssc.awaitTermination()
