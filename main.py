from Consumer.consumer import Consumer
from Producer.producer import Producer
from threading import Thread
import logging
import sys


def main():
    logging.basicConfig(level=logging.WARNING)
    logging.info("main Init")

    rabbitmq_host = sys.argv[1]
    db_path = sys.argv[2]
    folder_path = sys.argv[3]
    year = sys.argv[4]
    country = sys.argv[5]

    producer = Producer(rabbitmq_host,db_path,country,year)
    consumer = Consumer(rabbitmq_host,folder_path)
    producer.produce()
    try:
        t = Thread(target=consumer.consume)
        t.start()
        t.join

    except:
        logging.error ("Error: unable to start thread")

if __name__ == "__main__":
    main()


    

    