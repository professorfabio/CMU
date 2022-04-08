#from __future__ import print_function

import logging
import sys

import grpc
import iot_service_pb2
import iot_service_pb2_grpc


def run():
    with grpc.insecure_channel('34.136.25.200:50052') as channel:
        stub = iot_service_pb2_grpc.IoTServiceStub (channel)
        response = stub.CallAttribute(iot_service_pb2.AttributeRequest(
            session=int(sys.argv[1]),
            environment = sys.argv[2],
            attribute = sys.argv[3],
            parameter = sys.argv[4]
            ))
    
    print('Response:', response)

if __name__ == '__main__':
    logging.basicConfig()
    run()
