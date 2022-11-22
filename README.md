# Streaming_Sensor_Data_Dashboard
## Intro
This repo aims to demonstrate real-time dashboard application by following following steps:
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
## Overview
- Take a compressed data source from a URL
- Process the raw data with PySpark, and use HDFS as file storage, check resources with Apache Hadoop YARN.
- Use data-generator to simulate streaming data, and send the data to Apache Kafka.
- Read the streaming data from Kafka topic using PySpark (Spark Streaming).
- Write the streaming data to Elasticsearch, and visualize it using Kibana.
- Write the streaming data to MinIO (AWS Object Storage).
- Use Apache Airflow to orchestrate the whole data pipeline.
