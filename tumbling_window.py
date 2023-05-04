#!/usr/bin/python3

# imports
from kafka import KafkaProducer  # pip install kafka-python
import numpy as np  # pip install numpy
from sys import argv, exit
from time import time, sleep

# different device "profiles" with different
# distributions of values to make things interesting
# tuple --> (mean, std.dev)
DEVICE_PROFILES = {
    "Soyabean": {'bandi_market': (21.3, 37.7), 'city_market': (47.4, 58.7), 'grocery_shop': (101, 125.5) },
    "Barley": {'bandi_market': (29.5, 39.3), 'city_market': (32.0, 52.9), 'grocery_shop': (70.0, 90.3) },
    "Sorghum": {'bandi_market': (33.9, 61.7), 'city_market': (72.8, 91.8), 'grocery_shop': (96.9, 109.3) },
}

# check for arguments, exit if wrong
if len(argv) != 2 or argv[1] not in DEVICE_PROFILES.keys():
    print("please provide a valid device name:")
    for key in DEVICE_PROFILES.keys():
        print(f"  * {key}")
    print(f"\nformat: {argv[0]} DEVICE_NAME")
    exit(1)

profile_name = argv[1]
profile = DEVICE_PROFILES[profile_name]

# set up the producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

count = 1

# Define tumbling window duration and interval
window_duration = 10  # seconds
interval =  6 # seconds

# until ^C
while True:
    # Define a list to hold samples for the current window
    samples = []

    # Generate samples for the current window
    for i in range(int(window_duration / interval)):
        # get random values within a normal distribution of the value
        bandi_market = np.random.normal(profile['bandi_market'][0], profile['bandi_market'][1])
        city_market = max(0, min(np.random.normal(profile['city_market'][0], profile['city_market'][1]), 100))
        grocery_shop = np.random.normal(profile['grocery_shop'][0], profile['grocery_shop'][1])

        # create CSV structure
        msg = f'{time()},{profile_name},{bandi_market},{city_market},{grocery_shop}'

        # Add sample to the list
        samples.append(msg)

        # Sleep for the interval duration
        sleep(interval)

    # Send the entire window of samples to Kafka
    batch_msgs = ','.join(samples)
    producer.send('crop', bytes(batch_msgs, encoding='utf8'))
    count += 1
    print(f'sending data to kafka, #{count}')

    # Sleep for the remaining window duration
    sleep(window_duration - (int(window_duration / interval) * interval))


