#from __future__ import print_function

import logging

import grpc
import iot_service_pb2
import iot_service_pb2_grpc


def run():
    with grpc.insecure_channel('104.155.147.51:50051') as channel:
        stub = iot_service_pb2_grpc.IoTServiceStub(channel)
        response = stub.SayTemperature(iot_service_pb2.TemperatureRequest(sensorName='my_sensor'))

    print("Temperature received: " + response.temperature)

if __name__ == '__main__':
    logging.basicConfig()
    run()
