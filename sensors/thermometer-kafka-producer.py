import glob
import time
from kafka import KafkaProducer, KafkaConsumer
import math
import threading
import RPi.GPIO as GPIO # Import Raspberry Pi GPIO library

base_dir = '/sys/bus/w1/devices/'
device_folder = glob.glob(base_dir + '28*')[0]
device_file = device_folder + '/w1_slave'

# Initialize GPIO
GPIO.setwarnings(False) # Ignore warning for now
GPIO.setmode(GPIO.BOARD) # Use physical pin numbering
GPIO.setup(16, GPIO.OUT, initial=GPIO.LOW) # Set pin 16 to be an output pin and set initial value to low (off)

producer = KafkaProducer(bootstrap_servers='34.133.59.232:9092')
last_reported = 0

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
    consumer = KafkaConsumer(bootstrap_servers='34.133.59.232:9092')
    consumer.subscribe(topics=('ledcommand'))
    for msg in consumer:
        print ('Led command received: ', msg.value.decode())
        if msg.value == '1':
            print ('Turning led on')
            GPIO.output(16,GPIO.HIGH)
        else:
            print ('Turning led off')
            GPIO.output(16,GPIO.LOW)

trd =threading.Thread(target=consume_led_command)
trd.start()

while True:
    (temp_c, temp_f) = read_temp()
    print(temp_c, temp_f)
    if (math.fabs(temp_c - last_reported) >= 0.1):
        last_reported = temp_c
        producer.send('temperature', str(temp_c).encode())
    time.sleep(1)
