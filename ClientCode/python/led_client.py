#from __future__ import print_function

import logging
import sys

import grpc
import iot_service_pb2
import iot_service_pb2_grpc


def run():
    with grpc.insecure_channel('34.136.25.200:50051') as channel:
        stub = iot_service_pb2_grpc.IoTServiceStub (channel)
        response = stub.BlinkLed(iot_service_pb2.LedMessage(state=int(sys.argv[1])))

    if response.state == 1:
        print("Led state is on")
    else:
        print("Led state is off")

if __name__ == '__main__':
    logging.basicConfig()
    run()
