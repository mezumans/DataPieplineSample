import sqlite3
import pika
import pandas as pd
import logging
import random

import os

class Consumer():
    def __init__(self,rabbitmq_host,working_dir = os.getcwd):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_host))
        self.channel = self.connection.channel()
        self.declare_rabbitmq_queue()
        self.db_path = None
        self.working_dir = working_dir
        self.files_gen = FilesGenerator(self.working_dir)
        logging.info("Consumer started, folder path: {0}".format(working_dir))

    def declare_rabbitmq_queue(self):
        self.channel.queue_declare(queue='pipeline')
        logging.info("Queue declared , piepline")
        
    def callback(self,ch, method, properties, body):
        logging.info("Callback was called with args:{0}".format(body))
        self.db_path,year,country = self._parse_inputs(body)
        print(self.db_path)
        queries = Queries(year,country)
        conn = self.start_sql_connection(self.db_path)
        self._send_queries(queries,conn)
        
    def run_query(self,query,connection):
        logging.info("""Querry:
        {0}   was called""")
        return pd.read_sql_query(query,connection)

    def consume(self):
        self.channel.basic_consume(queue = 'pipeline',auto_ack=True,on_message_callback=self.callback)
        print("Consuming....")
        self.channel.start_consuming()

    def start_sql_connection(self,db_path): 
        try:   
            conn = sqlite3.connect(db_path)
            logging.info("Connection established with db path:{0}".format(db_path))
        except:
            conn = None
            logging.error("Error establishing sqlite connection")    
        return conn
    
    def _send_queries(self,queries,conn):
        for i in range(0,4):
            query = queries.get_query(i)
            df = self.run_query(query,conn)
            if i == 0 or i == 1:
                self.files_gen.generate_csv(df)
                self._create_table_from_query(str(i),query,conn)
            elif i == 2:
                self.files_gen.generate_json(df)    
            else:
                self.files_gen.generate_xml(df)    
                self._create_table_from_query(str(i),query,conn)
                
    def _create_table_from_query(self,table_name,query,conn):
        table_name = "Query{0}{1}".format( str(table_name),str(random.randint(0,100)))
        cursor = conn.cursor()
        query = 'CREATE TABLE {0} AS {1}'.format(table_name, query)
        print(query)
        cursor.execute(query)

    def _parse_inputs(self,body):
        body = body.decode("utf-8")
        inputs = body.split(',')
        db_path = inputs[0]
        year = inputs[1].replace(" ","")
        country = inputs[2].replace(" ","")
        return db_path,year,country 

class FilesGenerator():
    def __init__(self,path):
        self.working_dir = path
        self.file_num = 0
    
    def generate_csv(self,df):
        content = df.to_csv()
        self.save_file(content,'csv')

    def generate_xml(self,df):
        xml = '<Album>'
        cols = df.columns.tolist()
        rows = df.values.tolist()[0]
        for a,b in zip(rows,cols):
             xml = xml + ("\n   <{0}>{1}</{0}>".format(b,a))
        content = xml + '\n</Album>'
        self.save_file(content,'xml')
    
    def generate_json(self,df):
        content = df.to_json()
        self.save_file(content,'json')

    def save_file(self,content,file_suffix):
        full_path = "{0}\{1}{2}.{3}".format(self.working_dir,"resultedFile",self.file_num,file_suffix)
        self.file_num+=1
        with open(full_path,'w') as file:
            file.write(content)

class Queries():
    def __init__(self,year,country):
        self.year = year
        self.country = country

        QUERY0 =   """select customers.Country,count(invoices.invoiceid) from customers
                join invoices on customers.Customerid=invoices.Customerid group by (customers.country)"""

        QUERY1 = """select customers.country,count(invoice_items.invoicelineid) from 
                customers join invoices on customers.customerid = invoices.customerid
                    join invoice_items on invoice_items.invoiceid = invoices.invoiceid
                        group by customers.country"""

        QUERY2 = """select customers.country,GROUP_CONCAT(albums.title) from 
                    customers join invoices on customers.customerid = invoices.customerid
                    join invoice_items on invoice_items.invoiceid = invoices.invoiceid
                    join tracks on tracks.trackid = invoice_items.trackid
                    join albums on tracks.albumid = albums.albumid
                        group by customers.country"""

        QUERY3 = """select albums.title, customers.country,sum(invoice_items.quantity) as quan,strftime('%Y', invoicedate) as year1 from 
                customers join invoices on customers.customerid = invoices.customerid
                    join invoice_items on invoice_items.invoiceid = invoices.invoiceid
                    join tracks on tracks.trackid = invoice_items.trackid
                    join genres on genres.genreid = tracks.genreid
                    join albums on tracks.albumid = albums.albumid
                    where year1 = '{0}' and customers.country = '{1}' and genres.Name = 'Rock'
                    group by albums.title
                    order by quan desc
                    limit 1""".format(self.year,self.country)

        

        self.queries = [QUERY0,QUERY1,QUERY2,QUERY3]

    def get_query(self,query_num):
        return self.queries[query_num]


    