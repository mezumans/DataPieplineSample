import pandas as pd

class FilesGenerator():
    def __init__(self,path):
        self.folder_path = path
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
        print(content)
        self.save_file(content,'xml')
    
    def generate_json(self,df):
        content = df.to_json()
        self.save_file(content,'json')

    def save_file(self,content,file_suffix):
        full_path = "{0}{1}{2}.{3}".format(self.folder_path,"resultedFile",self.file_num,file_suffix)
        self.file_num+=1
        with open(full_path,'w') as file:
            file.write(content)
