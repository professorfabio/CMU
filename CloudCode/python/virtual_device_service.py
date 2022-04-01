from kafka import KafkaConsumer, KafkaProducer
import threading

from concurrent import futures
import logging
import secrets

import grpc
import iot_service_pb2
import iot_service_pb2_grpc

# Twin state
current_temperature = 'void'
led_state = {'red':0, 'green':0}
serial_keys = {} # key : devices
sessions = {} # session : user
users = {} # user : {password, serial_keys}

# Kafka consumer to run on a separate thread
def consume_temperature():
    global current_temperature
    consumer = KafkaConsumer(bootstrap_servers='35.226.115.184:9092')
    consumer.subscribe(topics=('temperature'))
    for msg in consumer:
        print (msg.value.decode())
        current_temperature = msg.value.decode()

def produce_led_command(state, ledname, session):
    producer = KafkaProducer(bootstrap_servers='35.226.115.184:9092')
    producer.send('ledcommand', key=ledname.encode(), value=str(state).encode(), sid=str(session).encode())
    return state
        
class IoTServer(iot_service_pb2_grpc.IoTServiceServicer):

    def SayTemperature(self, request, context):
        return iot_service_pb2.TemperatureReply(temperature=current_temperature)
    
    def BlinkLed(self, request, context):
        state = {}
        if request.session not in sessions:
            print('Session', request.session, 'rejected')
        else:
            print ("Blink led ", request.ledname)
            print ("...with state ", request.state)
            produce_led_command(request.state, request.ledname, request.session)
            # Update led state of twin
            led_state[request.ledname] = request.state
            state = led_state
        return iot_service_pb2.LedReply(ledstate=state)
    
    def RegisterDevices(self, request, context):
        print('Registering devices', request.device_keys, 'under serial key', request.serial_key)
        serial_keys[request.serial_key] = request.device_keys
        return iot_service_pb2.DeviceReply()
    
    def Login(self, request, context):
        if request.user not in users:
            users[request.user] = {
                'password': request.password,
                'serial_keys': set()
            }

        session = 0
        if users[request.user]['password'] == request.password:
            session = secrets.randbits(32)
            sessions[session] = request.user

        print('Login from user', request.user, 'returned session', session)
        return iot_service_pb2.LoginReply(session=session)

    def RegisterKey(self, request, context):
        devices = set()
        if request.session in sessions and request.serial_key in serial_keys:
            user = sessions[request.session]
            devices = serial_keys[request.serial_key]
            users[user]['serial_keys'].add(request.serial_key)
            print('User', user, 'acquired serial key', request.serial_key)
        else:
            print('Key registration rejected for session', request.session, 'and key', request.serial_key)

        return iot_service_pb2.KeyReply(device_keys=devices)

    def Authorize(self, request, context):
        authorized = request.session in sessions and request.serial_key in users[sessions[request.session]]['serial_keys']
        print('Authorization for session', request.session, 'to serial key', request.serial_key, 'is', authorized)

        return iot_service_pb2.AuthorizeReply(authorized=authorized)




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
    # Initialize the state of the leds on the actual device
    for color in led_state.keys():
        produce_led_command (led_state[color], color)
    serve()
