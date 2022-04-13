from kafka import KafkaConsumer, KafkaProducer
import threading

from concurrent import futures
import logging
import sys

import grpc
import iot_service_pb2
import iot_service_pb2_grpc
import id_provider_pb2
import id_provider_pb2_grpc

# Twin state
current_temperature = 'void'
sessions = {} # session -> {user, roles}
device_attributes = {} # device -> attributes
role_attributes = {
    'teacher': {'temperature', 'led_red', 'led_green', 'morse'},
    'student': {'temperature', 'led_green', 'morse'}
} # role -> attributes
environments = {
    'lab': {
        'devices': ['IoT_device'],
        'users': {'alice', 'bob'}
    }
} # environment -> {devices, users}

# Kafka consumer to run on a separate thread
def consume_temperature():
    global current_temperature
    consumer = KafkaConsumer(bootstrap_servers=sys.argv[2])
    consumer.subscribe(topics=('temperature'))
    for msg in consumer:
        print (msg.value.decode())
        current_temperature = msg.value.decode()

def produce_led_command(state, ledname):
    producer = KafkaProducer(bootstrap_servers=sys.argv[2])
    producer.send('ledcommand', key=ledname.encode(), value=str(state).encode())
    return state

def produce_morse_command(text):
    producer = KafkaProducer(bootstrap_servers=sys.argv[2])
    producer.send('morse', value=text.encode())

def call_attribute(environment, attribute, parameter):
    for device in environments[environment]['devices']:
        
        if device not in device_attributes or attribute not in device_attributes[device]:
            continue

        if attribute == 'temperature':
            return current_temperature

        if attribute == 'morse':
            produce_morse_command(parameter)
            return 'Outputting morse code'

        # led
        print ("Blink led ", attribute)
        print ("...with state ", parameter)
        produce_led_command(parameter, attribute)
        return 'Led blinked'
        
    return 'No device in the environment able to execute the called attribute'

def validate_AttributeRequest(request):
    if request.session not in sessions:
        with grpc.insecure_channel(sys.argv[1]) as channel:
            stub = id_provider_pb2_grpc.IdProviderStub (channel)
            response = stub.Session(id_provider_pb2.SessionRequest(session=request.session))
            if response.user != "":
                sessions[request.session] = {
                    'user': response.user,
                    'roles': response.roles
                }
                print('Verified session ', request.session)
                print('Current sessions: ', sessions)
            else:
                print('Session', request.session, 'rejected by identity provider')
                return False

    if request.environment not in environments:
        print('Environment', request.environment, 'does not exist')
        return False

    credentials = sessions[request.session]
    allowed_users = environments[request.environment]['users']
    if credentials['user'] not in allowed_users:
        print('User', credentials['user'], 'not authorized to access environment', request.environment)
        return False
    
    for role in credentials['roles']:
        if request.attribute in role_attributes[role]:
            return True
    
    print('User', credentials['user'], 'not authorized to access attribute', request.attribute)
    return False


class IoTServer(iot_service_pb2_grpc.IoTServiceServicer):

    def CallAttribute(self, request, context):
        value = 'ERROR Invalid call'
        if validate_AttributeRequest(request):
            value = call_attribute(request.environment, request.attribute, request.parameter)
        print('Attribute', request.attribute, 'call returned:', value)
        return iot_service_pb2.AttributeReply(value=value)
    
    def ConnectDevice(self, request, context):
        print('Adding device', request.device, 'with attributes', request.attributes, 'to the cloud')
        device_attributes[request.device] = request.attributes
        print('Current devices:', device_attributes)
        return iot_service_pb2.ConnectReply()


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    iot_service_pb2_grpc.add_IoTServiceServicer_to_server(IoTServer(), server)
    print('Registered environments:', environments)
    print('Registered roles:', role_attributes)
    print('Opening virtual device service on port 50052')
    server.add_insecure_port('[::]:50052')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()
    if len(sys.argv) == 3:
        trd = threading.Thread(target=consume_temperature)
        trd.start()
        serve()
    else:
        print('Expected arguments: [id_server] [bootstrap_server]')
        print('id_server - Hostname and port of the identity provider service')
        print('bootstrap_server - Hostname and port of the kafka broker')
