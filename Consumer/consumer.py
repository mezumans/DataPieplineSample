import sqlite3
import pika
import pandas as pd
from Queries.queries import Queries
from Queries.filesgen import FilesGenerator

class Consumer():
    def __init__(self,rabbitmq_host,country,year,file_path):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_host))
        self.channel = self.connection.channel()
        self.declare_rabbitmq_queue()
        self.db_path = None
        self.files_gen = FilesGenerator(file_path)

    def declare_rabbitmq_queue(self):
        self.channel.queue_declare(queue='pipeline')
        
    def callback(self,ch, method, properties, body):
        inputs = body.split(',')
        self.db_path = inputs[0]
        queries = Queries(inputs[1],inputs[2])
        conn = self.start_sql_connection(self.db_path)
        self._send_queries(queries,conn)
        
    def run_query(self,query,connection):
        return pd.read_sql_query(query,connection)

    def start_consuming(self,path):
        self.channel.basic_consume(queue = 'pipeline',auto_ack=True,on_message_callback=self.callback)

    def start_sql_connection(self,db_path):    
        conn = sqlite3.connect(db_path)
        return conn

    
    def _send_queries(self,queries,conn):
        for i in range(0,4):
            query = queries.get_query(i)
            df = self.run_query(query,conn)
            if i == 0 or i == 1:
                self.files_gen.generate_csv(df)
                self._create_table_from_query(str(i),query,conn)
            if i == 2:
                self.files_gen.generate_json(df)    
            else:
                self.files_gen.generate_xml(df)    
                self._create_table_from_query(str(i),query,conn)

    
    def _create_table_from_query(self,table_name,query,conn):
        cursor = conn.cursor()
        query = 'CREATE TABLE {0} AS' + query.format(table_name)
        cursor.execute(query)


