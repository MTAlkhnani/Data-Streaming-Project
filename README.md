# Data-Streaming-Project
This is a data streaming project that uses a python script to act as a producer, which basically sends a row to the Kafka topic every second. Then the other python script acts as a consumer and consumes the data every second and do analysis to it and then produce it to other topics in the same broker.

# Configurations:
1. Head over to this link: https://kafka.apache.org/downloads and download kafka
2. Starting Zookeeper
  a. Now in the Terminal or CMD head over to the kafka folder using and put
     the following command: bin/zookeeper-server-start.sh
     config/zookeeper.properties
4. Starting Kafka Server
  a. Open a new Terminal or CMD window, and head over to the kafka folder
     and put the following command: bin/kafka-server-start.sh
     config/server.properties

# For illustration purposes:
![image](https://github.com/MTAlkhnani/Data-Streaming-Project/assets/65413882/d4a0ba3b-7b95-4751-9618-0e2149b0700b)
