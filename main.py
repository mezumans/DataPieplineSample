from Consumer.consumer import Consumer
from Producer.producer import Producer
from threading import Thread
import sys


def main():
    producer = Producer('localhost','F:\JobInterviews\DataTask\chinook.db','USA','2009')
    consumer = Consumer('localhost','USA','2009','F:\JobInterviews\DataTask')
    producer.produce()
    try:
        t = Thread(target=consumer.consume)
        t.start()
    except:
        print ("Error: unable to start thread")

if __name__ == "__main__":
    main()


    

    