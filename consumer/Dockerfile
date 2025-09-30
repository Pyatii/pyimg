FROM python:3.7
RUN pip install pika pillow
ENV PYTHONUNBUFFERED=1
WORKDIR /usr/src/app/consumer
COPY consumer.py .
CMD [ "python", "consumer.py" ]