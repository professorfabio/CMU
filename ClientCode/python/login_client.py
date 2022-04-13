#from __future__ import print_function

import logging
import sys

import grpc
import id_provider_pb2
import id_provider_pb2_grpc

def run():
    with grpc.insecure_channel(sys.argv[1]) as channel:
        stub = id_provider_pb2_grpc.IdProviderStub (channel)
        response = stub.Login(id_provider_pb2.LoginRequest(user=sys.argv[2], password=sys.argv[3]))

    print('Login returned session', response.session)

if __name__ == '__main__':    
    if len(sys.argv) == 4:
        logging.basicConfig()
        run()
    else:
        print('Expected arguments: [id_server] [user] [password]')
        print('id_server - Hostname and port of the identity provider service')
        print('user - Login user')
        print('password - Login password')
