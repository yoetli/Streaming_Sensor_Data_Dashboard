# Streaming_Sensor_Data_Dashboard
## Intro
Organizations operate in dynamic environments that are constantly changing, such as industry fluctuations, changes in sales figures, or the impact of certain market news. This flow generates operational challenges that must continuously be monitored. In this context, this repo aims to demonstrate a real-time dashboard application over a dataset containing the data of the Smart Building System.


## Overview
- Get a compressed data source from a URL
- Processing the raw data with PySpark and saving the prepared dataset to local disk.
- Use the data-generator to simulate streamed batch data and send the data to Apache Kafka.
- Read the streaming data from Kafka topic using PySpark (Spark Structured Streaming).
- Write the streaming data to Elasticsearch, and visualize it using Kibana.

![image](https://user-images.githubusercontent.com/96085537/203293393-14d2c188-101a-49ac-92ed-604bcdd896bb.png)


## Used Technologies and Services
- Apache Spark (PySpark)
- Data Generator
- Apache Kafka
- Apache Zookeeper
- Elasticsearch
- Kibana
- Docker
- OS: Centos7
- IDE: Jupyter Lab, PyCharm
