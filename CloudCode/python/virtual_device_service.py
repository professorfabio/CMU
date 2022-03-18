from kafka import KafkaConsumer, KafkaProducer
import threading

from concurrent import futures
import logging

import grpc
import iot_service_pb2
import iot_service_pb2_grpc

current_temperature = 'void'

# Kafka consumer to run on a separate thread
def consume_temperature():
    global current_temperature
    consumer = KafkaConsumer(bootstrap_servers='35.226.115.184:9092')
    consumer.subscribe(topics=('temperature'))
    for msg in consumer:
        print (msg.value.decode())
        current_temperature = msg.value.decode()

def produce_led_command(state, ledname):
    producer = KafkaProducer(bootstrap_servers='35.226.115.184:9092')
    producer.send('ledcommand', key=str(ledname).encode(), value=str(state).encode())
    return state
        
class IoTServer(iot_service_pb2_grpc.IoTServiceServicer):

    def SayTemperature(self, request, context):
        return iot_service_pb2.TemperatureReply(temperature=current_temperature)
    
    def BlinkLed(self, request, context):
        print ("Blink led ", request.ledname)
        print ("...with state ", request.state)
        produce_led_command(request.state, request.ledname)
        return iot_service_pb2.LedMessage(state=request.state)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    iot_service_pb2_grpc.add_IoTServiceServicer_to_server(IoTServer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()
    trd = threading.Thread(target=consume_temperature)
    trd.start()
    serve()
