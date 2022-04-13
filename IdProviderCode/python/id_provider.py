from concurrent import futures
import logging
import secrets

import grpc
import id_provider_pb2
import id_provider_pb2_grpc

sessions = {} # session -> user
users = {
    'alice': {
        'password': '123',
        'roles': set(['teacher'])
    },
    'bob': {
        'password': '123',
        'roles': set(['student'])
    }
} # user -> {password, roles}

class IdProvider(id_provider_pb2_grpc.IdProviderServicer):
    
    def Login(self, request, context):
        session = 0
        if request.user in users and users[request.user]['password'] == request.password:
            session = secrets.randbits(32)
            sessions[session] = request.user

        print('Login from user', request.user, 'returned session', session)
        return id_provider_pb2.LoginReply(session=session)

    def Session(self, request, context):
        user = ""
        roles = []
        if request.session in sessions:
            user = sessions[request.session]
            roles = users[user]['roles']
            print('Session', request.session, 'contains user', user, 'with roles', roles)
        else:
            print('Session', request.session, 'is invalid')

        return id_provider_pb2.SessionReply(user=user, roles=roles)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    id_provider_pb2_grpc.add_IdProviderServicer_to_server(IdProvider(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()
    serve()
