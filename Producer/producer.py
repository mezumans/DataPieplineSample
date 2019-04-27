import sqlite3
import pika
import logging

class Producer():
    def __init__(self,rabbitmq_host,db_path,country,year):
       logging.basicConfig(level=logging.WARNING)
       self.init_rabbitmq_connection(rabbitmq_host)
       self.declare_rabbitmq_queue()
       self.db_path = db_path
       self.country = country
       self.year = year
       logging.info("Producer was created, RabbitMQ host: {0}, DB path: {1}, Query on countr:{2} on: {3}".format(rabbitmq_host,db_path,country,year))

    def init_rabbitmq_connection(self,rabbitmq_host):   
        self.params = pika.ConnectionParameters(host = rabbitmq_host,heartbeat=600,blocked_connection_timeout=300)
        try:
            self.connection = pika.BlockingConnection(self.params)
            self.channel = self.connection.channel()
            logging.info("Connection established with RabbitMQ")
        except:
            logging.error("Error occured while trying to connect to RabbitMQ")
            
        
    def declare_rabbitmq_queue(self):
         self.channel.queue_declare(queue='pipeline')
         logging.info("Queue named pipeline declared")

    def produce(self):
        self.channel.basic_publish(exchange='',
                            routing_key='pipeline',
                            body="{}, {}, {}".format(self.db_path,self.year,self.country))

        
        self.connection.close()