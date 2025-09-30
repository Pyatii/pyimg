import pika
import base64


url_params = pika.URLParameters('amqp://rabbit_mq?connection_attempts=10&retry_delay=10')

connection = pika.BlockingConnection(url_params)

channel = connection.channel()

queue_name = 'my_queue'
channel.queue_declare(queue=queue_name)


def callback(ch, method, properties, body):
    print(f"Received message {body}")
    with open("/usr/src/app/consumer/photo.jpg", "wb") as file:
        file.write(base64.b64decode(body))


def consume():
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    print('waiting...')
    channel.start_consuming()

consume()

channel.close()

connection.close()