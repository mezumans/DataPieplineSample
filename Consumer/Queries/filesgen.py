import pandas as pd

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
