import sqlite3
import pika
import pandas as pd

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

    def consume(self):
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


class FilesGenerator():
    def __init__(self,path):
        self.path = path
    
    def generate_csv(self,df):
        content = df.to_csv()
        self.save_file(content,'csv')

    def generate_xml(self,df):
        xml = '<Album>'
        cols = df.columns.tolist()
        rows = df.values.tolist()[0]
        for a,b in zip(rows,cols):
             xml + ("\n   <{0}>{1}</{0}>".format(b,a))
        content = xml + '</Album>'
        self.save_file(content,'xml')
    
    def generate_json(self,df):
        content = df.to_json()
        self.save_file(content,'json')

    def save_file(self,file_suffix,content):
        with open(self.path+file_suffix,'w') as file:
            file.write(file)

class Queries():
    def __init__(self,year,country):
        self.year = year
        self.country = country

        QUERY0 =   """select customers.Country,count(invoices.invoiceid) from customers
                join invoices on customers.Customerid=invoices.Customerid group by (customers.country)"""

        QUERY1 = """select customers.country,count(invoice_items.invoicelineid) from 
                customers join invoices on customers.customerid = invoices.customerid
                    join invoice_items on invoice_items.invoiceid = invoices.invoiceid
                        group by customers.country'"""

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
                    where year1 = {} and customers.country = {} 'and genres.Name = 'Rock'
                    group by albums.title
                    order by quan desc
                    limit 1""".format(self.year,self.country)

        

        self.queries = [QUERY0,QUERY1,QUERY2,QUERY3]

    def get_query(self,query_num):
        return self.queries[query_num]


    