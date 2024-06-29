# stream-processing


To launch this, make sure you already installed all the necessaries. Then you just have to run these 4 steps:

### Start ZooKeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

### Start Kafka
bin/kafka-server-start.sh config/server.properties

### Set environment variables
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service/account/key.json"

### Start producer
python producer.py

### Start consumer
python consumer.py
