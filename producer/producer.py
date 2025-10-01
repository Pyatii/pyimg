import pika
import time
import base64


url_params = pika.URLParameters('amqp://rabbit_mq?connection_attempts=10&retry_delay=10')

connection = pika.BlockingConnection(url_params)

channel = connection.channel()

exchange_name = 'direct_log'
exchange_type = 'direct'

channel.exchange_declare(exchange=exchange_name, exchange_type=exchange_type)

queue_name = 'my_queue'

channel.queue_declare(queue=queue_name)

routing_key = 'info'
channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=routing_key)

with open("toples.jpg", "rb") as file:
    img_64 = file.read()
message = base64.b64encode(img_64)

channel.basic_publish(exchange=exchange_name, routing_key=routing_key, body=message)
print("Фото отправлено")

channel.close()

connection.close()