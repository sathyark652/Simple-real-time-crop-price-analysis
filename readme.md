sudo systemctl start zookeeper
sudo systemctl start kafka
sudo systemctl start mongod

Create a kafka topic 'crop'

run the dbt_kafka.py 
run the streaming.py (streaming mode)

run tumbling.py and sliding.py (windows)

see the results in terminal and it will be stored in mongodb 

run the batch_test.py for batch processing 
