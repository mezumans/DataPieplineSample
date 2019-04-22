from Consumer.consumer import Consumer
from Producer.producer import Producer
import thread
import sys

if __name__ == "__main__":
    main()

def main():
    producer = Producer('localhost')
    consumer = Consumer('localhost','USA','2009','F:\JobInterviews\DataTask')
    

    