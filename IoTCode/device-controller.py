import glob
import time
import sys
from kafka import KafkaProducer, KafkaConsumer
from time import sleep
import math
import threading
import requests
import RPi.GPIO as GPIO # Import Raspberry Pi GPIO library

import grpc
import iot_service_pb2
import iot_service_pb2_grpc

base_dir = '/sys/bus/w1/devices/'
device_folder = glob.glob(base_dir + '28*')[0]
device_file = device_folder + '/w1_slave'

# Initialize GPIO
GPIO.setwarnings(False) # Ignore warning for now
GPIO.setmode(GPIO.BOARD) # Use physical pin numbering
GPIO.setup(16, GPIO.OUT, initial=GPIO.LOW) # Set pin 16 to be an output pin and set initial value to low (off)
GPIO.setup(18, GPIO.OUT, initial=GPIO.LOW) # Idem for pin 18

producer = KafkaProducer(bootstrap_servers=sys.argv[2] + ':9092')
last_reported = 0

morse_table = {
    'a': '. _',
    'b': '_ . . .',
    'c': '_ . _ .',
    'd': '_ . .',
    'e': '.',
    'f': '. . _ .',
    'g': '_ _ .',
    'h': '. . . .',
    'i': '. .',
    'j': '. _ _ _',
    'k': '_ . _',
    'l': '. _ . .',
    'm': '_ _',
    'n': '_ .',
    'o': '_ _ _',
    'p': '. _ _ .',
    'q': '_ _ . _',
    'r': '. _ .',
    's': '. . .',
    't': '_',
    'u': '. . _',
    'v': '. . . _',
    'w': '. _ _',
    'x': '_ . . _',
    'y': '_ . _ _',
    'z': '_ _ . .',
    '1': '. _ _ _ _',
    '2': '. . _ _ _',
    '3': '. . . _ _',
    '4': '. . . . _',
    '5': '. . . . .',
    '6': '_ . . . .',
    '7': '_ _ . . .',
    '8': '_ _ _ . .',
    '9': '_ _ _ _ .',
    '0': '_ _ _ _ _',
    ' ': ' '
}

def morse(text):
    morse = ''
    for c in ' '.join(text.lower().split()):
        if c in morse_table:
            morse += morse_table[c] + '   '
    return morse

def read_temp_raw():
    f = open(device_file, 'r')
    lines = f.readlines()
    f.close()
    return lines

def read_temp():
    lines = read_temp_raw()
    while lines[0].strip()[-3:] != 'YES':
        time.sleep(0.2)
        lines = read_temp_raw()
    equals_pos = lines[1].find('t=')
    if equals_pos != -1:
        temp_string = lines[1][equals_pos+2:]
        temp_c = float(temp_string) / 1000.0
        temp_f = temp_c * 9.0 / 5.0 + 32.0
        return temp_c, temp_f

def consume_led_command():
    with grpc.insecure_channel(sys.argv[1] + ':50052') as channel:
        stub = iot_service_pb2_grpc.IoTServiceStub (channel)
        response = stub.ConnectDevice(iot_service_pb2.ConnectRequest(device='IoT_device', attributes=['led_red', 'led_green', 'temperature', 'morse']))

    consumer = KafkaConsumer(bootstrap_servers=sys.argv[2] + ':9092')
    consumer.subscribe(topics=('ledcommand'))
    ledpin = 0
    for msg in consumer:
        print ('Led command received: ', msg.value)
        print ('Led to blink: ', msg.key)
        if msg.key == b'led_red':
            ledpin = 16
        else:
            ledpin = 18
        if msg.value == b'1':
            print ('Turning led on')
            GPIO.output(ledpin,GPIO.HIGH)
        else:
            print ('Turning led off')
            GPIO.output(ledpin,GPIO.LOW)

def consume_morse_command():
    consumer = KafkaConsumer(bootstrap_servers=sys.argv[2] + ':9092')
    consumer.subscribe(topics=('morse'))

    for msg in consumer:
        text = msg.value.decode()
        print ('Morse command received:', text)

        code = ''
        if len(text) < 20:
            code = morse(text)
        else:
            print('Offloading morse encoding to cloud function')
            r = requests.get('http://us-central1-cmu-2021-2.cloudfunctions.net/morse', params={'message': text})
            code = r.text

        print ('Output:', code)

        ledpin = 18
        dit = .3
        for c in code:
            if c == ' ':
                GPIO.output(ledpin, GPIO.LOW)
            else:
                GPIO.output(ledpin, GPIO.HIGH)
            
            if c == '_':
                sleep(3 * dit)
            else:
                sleep(dit)

trd =threading.Thread(target=consume_led_command)
trd.start()
trd = threading.Thread(target=consume_morse_command)
trd.start()

while True:
    (temp_c, temp_f) = read_temp()
    print(temp_c, temp_f)
    if (math.fabs(temp_c - last_reported) >= 0.1):
        last_reported = temp_c
        producer.send('temperature', str(temp_c).encode())
    time.sleep(1)
