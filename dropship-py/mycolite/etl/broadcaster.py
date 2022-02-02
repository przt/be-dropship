import pika
import json
import logging

# create connection
# connection = pika.BlockingConnection(pika.ConnectionParameters(host='172.17.0.2'))
# channel = connection.channel()
# create a queue
# channel.queue_declare(queue = 'mycomsg')
# publish a message
# channel.basic_publish(exchange='', routing_key='mycomsg', body='Hello MYCO!')
# print(" [x] Sent 'Hello MYCO!'")
# close the connection
# connection.close()

class Publish():

    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue = 'mycomsg')

    def connect(self):
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue = 'mycomsg')
        except Exception as e:
            print(e)

    def check_conn(self):
        return self.connection.is_open

    def send(self, msg):
        try:
            self.channel.basic_publish(exchange='', routing_key='mycomsg', body=json.dumps(msg).encode())
            logging.debug(f" Sent: {msg}")
        except (Exception, pika.exceptions.ConnectionClosed) as e:
            logging.debug('connection closed. reconnecting...')
            if self.check_conn():
                self.channel.basic_publish(exchange='', routing_key='mycomsg', body=json.dumps(msg).encode())
                logging.debug(f" Sent: {msg}")

    def close_channel(self):
        self.connection.close()
        return self.connection.is_open
