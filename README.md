# Streaming_Sensor_Data_Dashboard
## Introduction
Organizations operate in dynamic environments that are constantly changing, such as industry fluctuations, changes in sales figures, or the impact of certain market news. This flow generates operational challenges that must continuously be monitored. In this context, this repo aims to demonstrate a real-time dashboard application over a streaming dataset containing the data of the Smart Building System.

## Process Flow
- Geting a compressed data source from a URL
- Processing the raw data with PySpark and saving the prepared dataset to local disk.
- Use the data-generator to simulate streamed batch data and send the data to Apache Kafka.
- Read the streaming data from Kafka topic using PySpark (Spark Structured Streaming).
- Write the streaming data to Elasticsearch, and visualize it using Kibana.

![](C:\Users\oner\Desktop\final_pro_pic.png)

## Used Tools

- Apache Spark (PySpark)
- Data Generator
- Apache Kafka
- Apache Zookeeper
- Elasticsearch
- Kibana
- Docker
- OS: Centos7
- IDE: Jupyter Lab, PyCharm

## Geting a compressed data source from a URL


```{r}
!wget -O ~/datasets/sensor/sensor_df.zip https://github.com/erkansirin78/datasets/raw/master/sensors_instrumented_in_an_office_building_dataset.zip
```
```
!unzip sensor_df.zip
```
The unzipped file KETI contains sub folders labeled with room numbers of the building and a README.txt file. Each room contains five csv files, and each file shows logged data of a sensor. Csv files also include timestamp column.

- co2.csv
- humidity.csv
- light.csv
- temperature.csv
- pir.csv

## Getting the things started
Apache Spark, Apache Kafka, Zookeeper, Docker Compose Jupyter Lab, PyCharm locally installed.
Elasticsearch and Kibana have been run via docker-compose.yaml.
```
sudo systemctl start zookeeper
sudo systemctl status zookeeper
sudo systemctl start kafka
sudo systemctl status kafka
``` 
## Processing of the raw data with PySpark

## Creating the Kafka Topic:
``` 
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic sensors-topic --partitions 1 --replication-factor 1 
kafka-topics.sh --bootstrap-server localhost:9092 --list
``` 
## Data-Generator:

Benefiting from the data-generator, the static dataset will be turned into streaming dataset and delivered to the topic of Apache Kafka.  

Instructions on how to install data-generator can be found [here](https://github.com/erkansirin78/data-generator).

Kafka (dataframe_to_kafka.py)

We can directly run the data-generator script by running data_generator.sh. We should use the location of data-generator.
```
python dataframe_to_kafka.py -ks , -rst 0.1 -t sensors-topic -i /home/train/datasets/sensor_raw_dataset/datagen_input
```
Streaming data example:

## Running the Read-Write PySpark
