from kafka import KafkaConsumer
import threading

from concurrent import futures
import logging

import grpc
import temperature_service_pb2
import temperature_service_pb2_grpc

current_temperature = ''

# Kafka consumer to run on a separate thread
def consume_temperature():
    consumer = KafkaConsumer(bootstrap_servers='34.133.59.232:9092')
    consumer.subscribe(topics=('temperature'))
    for msg in consumer:
        print (msg.value.decode())
        current_temperature = msg.value.decode()

class TemperatureServer(temperature_service_pb2_grpc.TemperatureServiceServicer):

    def SayTemperature(self, request, context):
        return temperature_service_pb2.TemperatureReply(temperature=current_temperature)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    temperature_service_pb2_grpc.add_TemperatureServiceServicer_to_server(TemperatureServer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()
    trd =threading.Thread(target=consume_temperature)
    trd.start()
    serve()