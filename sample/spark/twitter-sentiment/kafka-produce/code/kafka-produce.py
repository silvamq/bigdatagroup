from kafka import KafkaProducer
import time

time.sleep(30)

producer = KafkaProducer(bootstrap_servers='kafka:9092')
topic = 'topic_in'

msgs = []
msgs.append(b'Bitcoin caiu')
msgs.append(b'BTC sobe')
msgs.append(b'Bitcoin sobe')
msgs.append(b'BTC caiuuu')
msgs.append(b'ETH reage')
msgs.append(b'BTC e ETH reage juntos')

i = 0
while True:
    for msg in msgs:
        producer.send(topic, msg)
        time.sleep(i % 3)
        i = i+1
        print(msg)
