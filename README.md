# Data-Streaming-Project
This is a data streaming project that uses a python script to act as a producer, which basically sends a row to the Kafka topic every second. Then the other python script acts as a consumer and consumes the data every second and do analysis to it and then produce it to other topics in the same broker.

# Configurations:
1. Head over to this link: https://kafka.apache.org/downloads and download Kafka
2. Starting Zookeeper<br />
  a. Now in the Terminal or CMD head over to the kafka folder using and put
     the following command: bin/zookeeper-server-start.sh
     config/zookeeper.properties
4. Starting Kafka Server<br/>
  a. Open a new Terminal or CMD window, and head over to the kafka folder
     and put the following command: bin/kafka-server-start.sh
     config/server.properties
5. Head over to this link: https://spark.apache.org/downloads.html and download Spark<br/>
   a. Now this is for the second code which is (data_process.py) and to run it, open a new Terminal or CMD and put the following command: ./spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 --master 'local[2]' 
*/Users/mohammedalkhnani/Kafka/PySpark/data_process.py* Note: You have to put the destination of the python file here <br/>  
# For illustration purposes:
![image](https://github.com/MTAlkhnani/Data-Streaming-Project/assets/65413882/d4a0ba3b-7b95-4751-9618-0e2149b0700b)
