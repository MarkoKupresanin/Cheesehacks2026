from kafka import KafkaConsumer
producer = KafkaProducer(bootstrap_servers = '202.215.0.218')

topic = "frames"


def success_callback(s):
    print("Send to redpanda.")
    print(s)


def fail_callback(e):
    print("Issue with sending to redpanda:")
    print(e)

def send_to_redpanda(data):
    future = producer.send(
                topic,
                value=data
            )
    future.add_callback(success_callback)
    future.add_errback(fail_callback)
