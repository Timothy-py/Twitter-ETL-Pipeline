from kafka import KafkaConsumer
import time
import json

topic_name = 'timothy_twitter_stream_topic'

# instantiate and configure the kafka consumer class
consumer = KafkaConsumer(topic_name, bootstrap_servers='52.19.199.252:9092,52.19.199.252:9093,52.19.199.252:9094',
                         auto_offset_reset='earliest',
                         enable_auto_commit=False)

# simple print statement to determine if the kafka consumer object is well set
print('Consumer ready = {}'.format(consumer))

# a json file with a time stamp to save each tweets
fileObj = open('timothy_twitter_stream_topic_{}.json'.format(str(time.time())), 'w')

# loop through all the data in kafka, and write each data into the timestamped json file.
for message in consumer:
    jsonFile = json.dumps(message.value.decode('UTF-8'))
    fileObj.write(jsonFile)
    print('Done!')


