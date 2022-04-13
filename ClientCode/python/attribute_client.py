#from __future__ import print_function

import logging
import sys

import grpc
import iot_service_pb2
import iot_service_pb2_grpc


def run():
    with grpc.insecure_channel(sys.argv[1]) as channel:
        stub = iot_service_pb2_grpc.IoTServiceStub (channel)
        parameter = ''
        if sys.argc >= 6:
            parameter = sys.argv[5]
        response = stub.CallAttribute(iot_service_pb2.AttributeRequest(
            session=int(sys.argv[2]),
            environment = sys.argv[3],
            attribute = sys.argv[4],
            parameter = parameter
            ))
    
    print('Response:', response.value)

if __name__ == '__main__':
    if sys.argc == 5 or sys.argc == 6:
        logging.basicConfig()
        run()
    else:
        print('Expected arguments: [iot_server] [session] [environment] [attribute] [<parameter>]')
        print('iot_server - Hostname and port of the virtual device service')
        print('session - Session identifier')
        print('environment - Environment identifier')
        print('attribute - Attribute to call')
        print('parameter - Parameter string for attribute call')
