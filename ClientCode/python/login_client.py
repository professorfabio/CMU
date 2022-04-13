#from __future__ import print_function

import logging
import sys

import grpc
import id_provider_pb2
import id_provider_pb2_grpc

def run():
    with grpc.insecure_channel(sys.argv[1] + ':50051') as channel:
        stub = id_provider_pb2_grpc.IdProviderStub (channel)
        response = stub.Login(id_provider_pb2.LoginRequest(user=sys.argv[2], password=sys.argv[3]))

    print('Login returned session', response.session)

if __name__ == '__main__':
    logging.basicConfig()
    run()
