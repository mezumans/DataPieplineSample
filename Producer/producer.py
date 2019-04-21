import sqlite3
import pika

class Produceer():
    def __init__(self,rabbitmq_host):
       self.init_rabbitmq_connection(rabbitmq_host)
       self.declare_rabbitmq_queue()

    def init_rabbitmq_connection(self,rabbitmq_host):   
        self.params = pika.ConnectionParameters(host = rabbitmq_host,heartbeat=600,blocked_connection_timeout=300)
        self.connection = pika.BlockingConnection(self.params)
        self.channel = self.connection.channel()
        
    def declare_rabbitmq_queue(self):
         self.channel.queue_declare(queue='pipeline')

    def produce(self,db_path,country,year):
        self.channel.basic_publish(exchange='',
                            routing_key='pipeline',
                            body="{}, {}, {}".format(db_path,year,country))

        

