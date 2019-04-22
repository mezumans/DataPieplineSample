import sqlite3
import pika

class Producer():
    def __init__(self,rabbitmq_host,db_path,country,year):
       self.init_rabbitmq_connection(rabbitmq_host)
       self.declare_rabbitmq_queue()
       self.db_path = db_path
       self.country = country
       self.year = year

    def init_rabbitmq_connection(self,rabbitmq_host):   
        self.params = pika.ConnectionParameters(host = rabbitmq_host,heartbeat=600,blocked_connection_timeout=300)
        self.connection = pika.BlockingConnection(self.params)
        self.channel = self.connection.channel()
        
    def declare_rabbitmq_queue(self):
         self.channel.queue_declare(queue='pipeline')

    def produce(self):
        self.channel.basic_publish(exchange='',
                            routing_key='pipeline',
                            body="{}, {}, {}".format(self.db_path,self.year,self.country))

        
