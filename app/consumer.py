from kafka import KafkaConsumer
import json

import logging
#paramters
topic= "test"
bootstrap_server = "localhost:9092"
group_name = "message_consumer_group"

consumer = KafkaConsumer(topic,
                        bootstrap_servers = bootstrap_server,
                        group_id = group_name,
                        value_deserializer= lambda v: json.loads(v.decode("utf-8"))
                            
)

consumer.subscribe(topics=[topic])
try:
    for message in consumer:
        print(message.value)
except Exception as e:
    logging.error("error --------> %s", str(e))
finally:
    consumer.close()