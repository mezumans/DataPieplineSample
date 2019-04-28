#python version 3.6.1
import sys
import producer

def main():
    rabbitmq_host = sys.argv[1]
    producer1 = producer.Producer(rabbitmq_host)
    producer1.produce(sys.argv[2],sys.argv[3],sys.argv[4])
    
if __name__ == "__main__":
    main()