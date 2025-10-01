import pika
import base64
from PIL import Image
from io import BytesIO

url_params = pika.URLParameters('amqp://rabbit_mq?connection_attempts=10&retry_delay=10')

connection = pika.BlockingConnection(url_params)

channel = connection.channel()

queue_name = 'my_queue'
channel.queue_declare(queue=queue_name)


def callback(ch, method, properties, body):
    image = Image.open(BytesIO(base64.b64decode(body)))
    image.save("/usr/src/app/consumer/images/photo_original.jpg", "JPEG")
    image = image.resize((image.width // 10, image.height // 10))
    image.save("/usr/src/app/consumer/images/photo.jpg", "JPEG")


def consume():
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    print('waiting...')
    channel.start_consuming()

consume()

channel.close()

connection.close()