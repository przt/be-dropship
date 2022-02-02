import pika
import urllib.request

class Subscribe():
    def callback(self, ch, method, properties, body):
        try:
            message = str(body).replace(' ', '%20')
            print(message)
            with urllib.request.urlopen(f"http://localhost:8000/push/{message}") as f:
                print(f.read(100))
        except Exception as e:
            print(e)

    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='mycomsg')
        self.channel.basic_consume(queue = 'mycomsg', on_message_callback=self.callback, auto_ack=True)

        print(' [*] Waiting for messages. To exit press CTRL+C')
        try:
            self.channel.start_consuming()
        except Exception as e:
            print(e)
        except KeyboardInterrupt as ke:
            print(' Shutdown listener...')
            exit(0)

subscribe = Subscribe()
