from kafka import KafkaConsumer

consumer = KafkaConsumer(bootstrap_servers='34.133.59.232:9092')
consumer.subscribe(topics=('temperature'))
for msg in consumer:
    print (msg)