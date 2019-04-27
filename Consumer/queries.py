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