#python version 3.6.1
import consumer
import sys
import os

def main():
    rabbitmq_host = sys.argv[1]
    working_dir = os.getcwd()
    if(len(sys.argv)>2):
        working_dir = sys.argv[2]   
    consumer1 = consumer.Consumer(rabbitmq_host,working_dir)    
    consumer1.consume()

if __name__ == "__main__":
    main()
