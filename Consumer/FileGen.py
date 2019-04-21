import pandas as pd

class FilesGenerator():
    def __init__(self,path):
        self.path = path
        

    def generate_csv(self,df):
        csv = df.to_csv()
        self.save_file(csv)

    def generate_xml(self):


    def generate_json(self,df):
        json = df.to_json()
        self.save_file(json)



    def save_file(f):
        with open(self.path,w) as file:
            file.write(f)