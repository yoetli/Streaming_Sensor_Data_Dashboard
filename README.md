# Streaming_Sensor_Data_Dashboard
## Intro
Organizations operate in dynamic environments that are constantly changing, such as industry fluctuations, changes in sales figures, or the impact of certain market news. This flow generates operational challenges that must continuously be monitored. 

In this context, this repo aims to demonstrate a real-time dashboard application over a dataset containing the data of the Smart Building System.


## Overview
- Get a compressed data source from a URL
- Process the raw data with PySpark, write the prepared dataset to local disk.
- Use the data-generator to simulate streamed batch data, and send the data to Apache Kafka.
- Read the streaming data from Kafka topic using PySpark (Spark Structured Streaming).
- Write the streaming data to Elasticsearch, and visualize it using Kibana.

Dataset Preparation by PySpark, Streaming Data Generation, Reading from Kafka, PySpark Modifications, Writing to Elastic Search, Visualization of Kibana Graphs

## Used Technologies and Services
- Apache Spark (PySpark)
- Data Generator
- Apache Kafka
- Apache Zookeeper
- Elasticsearch
- Kibana
- Docker
-----------
- OS: Centos7
- IDE: Jupyter Lab, PyCharm


Dataset Preparation by PySpark, Streaming Data Generation, Reading from Kafka, PySpark Modifications, Writing to Elastic Search, Visualization of Kibana Graphs
