from kafka import KafkaConsumer
import time, json
topic_name = 'timothy_twitter_stream_topic'
consumer = KafkaConsumer(topic_name, bootstrap_servers='52.19.199.252:9092,52.19.199.252:9093,52.19.199.252:9094',
                         auto_offset_reset='earliest',
                         enable_auto_commit=False)

print('Consumer ready = {}'.format(consumer))

fileObj = open('timothy_twitter_stream_topic_{}.json'.format(str(time.time())), 'w')

for message in consumer:
    jsonFile = json.dumps(message.value.decode('UTF-8'))
    fileObj.write(jsonFile)
    # fileObj.close()
    print('Done!')


