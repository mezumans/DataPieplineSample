import sqlite3
import pika
import pandas as pd
import queries


class consumer():
    def __init__(self,rabbitmq_host,country,year):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_host))
        self.channel = self.connection.channel()
        self.declare_rabbitmq_queue()
        self.db_path = None

    def declare_rabbitmq_queue(self):
        self.channel.queue_declare(queue='pipeline')
        
    def callback(self,ch, method, properties, body):
        inputs = body.split(',')
        self.db_path = inputs[0]
        queries = queries.Queries(inputs[1],inputs[2])
        conn = self.start_sql_connection(self.db_path)
        for i in range(0,4):
            query = queries.get_query(i)
            self.run_query(query,conn)

    def run_query(self,query,connection):
        return pd.read_sql_query(query,connection)

    def start_consuming(self,path):
        self.channel.basic_consume(queue = 'pipeline',auto_ack=True,on_message_callback=self.callback)

    def start_sql_connection(self,db_path):    
        conn = sqlite3.connect(db_path)
        return conn


    
