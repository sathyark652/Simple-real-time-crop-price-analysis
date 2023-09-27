# Real-Time Crop Price Analysis

Real-Time Crop Price Analysis is a project designed to assist farmers in rural areas by providing real-time crop price information using Apache Kafka, Spark, Spark SQL, and MongoDB. This tool helps farmers make informed decisions about when to sell their crops based on market prices.

## Purpose
This project aims to provide rural farmers with essential information to make informed decisions about selling their crops. It bridges the gap between farmers and market prices, helping them maximize their profits.

## Getting Started

Follow these steps to set up and run the project:

### Prerequisites

- Ensure you have Apache Kafka, Apache Spark, and MongoDB installed and running.
- Start Zookeeper, Kafka, and MongoDB services:

   ```bash
   sudo systemctl start zookeeper
   sudo systemctl start kafka
   sudo systemctl start mongod


Create a Kafka topic named 'crop':
  
   kafka-topics.sh --create --topic crop --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1


## Streaming Mode

Run the data producer dbt_kafka.py to feed data into the Kafka topic:


python dbt_kafka.py
Execute the streaming data processing using streaming.py:


python streaming.py
Implement sliding window and tumbling window analysis with sliding.py and tumbling.py.

## Batch Mode
For batch processing, run batch_test.py:
 
python batch_test.py

## Results

View the real-time crop price analysis results in the terminal. All data is stored in MongoDB for further reference.


