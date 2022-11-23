# Streaming_Sensor_Data_Dashboard
## Introduction
Organizations operate in dynamic environments that are constantly changing, such as industry 
fluctuations, changes in sales figures, or the impact of certain market news. 
This flow generates operational challenges that must continuously be monitored. 
In this context, this repo aims to demonstrate a real-time dashboard application over a streaming
dataset containing the data of the Smart Building System.

## Process Flow
- Geting a compressed data source from a URL
- Processing the raw data with PySpark and saving the prepared dataset to local disk.
- Use the data-generator to simulate streamed batch data and send the data to Apache Kafka.
- Reading the streaming data from Kafka topic by PySpark.
- Writing the streaming data from Kafka to Elasticsearch by PySpark.
- Observation of the index & docs in Elasticsearch and creation of Kibana Dashboard.

![](C:\Users\oner\Desktop\final_pro_pic2.jpg)

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


```shell
!wget -O ~/datasets/sensor/sensor_df.zip https://github.com/erkansirin78/datasets/raw/master/sensors_instrumented_in_an_office_building_dataset.zip
```
```shell
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
```shell
sudo systemctl start zookeeper
sudo systemctl start kafka
sudo systemctl status kafka
sudo systemctl start docker
cd /<location_of_docker_compose.yaml>/ && docker-compose up -d
``` 
## Processing of the raw data with PySpark

Please refer to ``` Read_Write_Static_Data.ipynb``` for pre-processing of log data.
Some of the code used to modify the initial data shown below. 

```python
start_time = time.time()

for n in range(0,len(dirpaths),5):
    for i in range(len(names)):
        df=(spark.read
             .option("header", False)
             .format("csv")
             .schema(list_schema[i])
             .load(f"file://{dirpaths[n]}/{names[i]}")
           )
        if i == 0:
            df_co2=df.withColumn("room",F.lit((f"file://{dirpaths[n]}").split("/")[-1]))\
                    .withColumn("event_ts_min",F.to_timestamp(F.from_unixtime(F.col("ts_min_bignt"),'yyyy-MM-dd HH:mm:ss')))
        elif i == 1:
            df_humidity =df
        elif i == 2:
            df_light = df
        elif i == 3:
            df_pir = df
        else:
            df_temperature  = df

    df_ch=df_co2.join(df_humidity,(df_co2["ts_min_bignt"] == df_humidity["tmstmp1"]),"inner")
    df_chl = df_ch.join(df_light, (df_ch["ts_min_bignt"] == df_light["tmstmp2"]),"inner")
    df_chlp = df_chl.join(df_pir, (df_chl["ts_min_bignt"] == df_pir["tmstmp3"]),"inner")
    df_full= df_chlp.join(df_temperature, (df_chlp["ts_min_bignt"] == df_temperature["tmstmp4"]),"inner")

    df_full.repartition(1).write\
                      .format("csv")\
                      .mode("append")\
                      .option("header", True)\
                      .save("file:///home/train/datasets/sensor_raw_dataset/output/")

print("----- %s secs -----" %(time.time() - start_time))
```


## Creating the Kafka Topic:
``` shell
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic sensors-topic --partitions 1 --replication-factor 1 
kafka-topics.sh --bootstrap-server localhost:9092 --list
``` 
## Data-Generator:

Benefiting from the data-generator, the static dataset will be turned into streaming dataset and delivered to the topic of Apache Kafka.  

Instructions on how to install data-generator can be found [here](https://github.com/erkansirin78/data-generator).

```python
python dataframe_to_kafka.py -ks , -rst 0.1 -t sensors-topic -i /home/train/datasets/sensor_raw_dataset/datagen_input
```

##  Reading the streaming data from Kafka topic by PySpark

Please refer to ``` Read_from_kafka.py``` for details of how to read the streaming data.
Some of the code used to read the streaming data shown below. 

```python
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "office-input") \
  .option("failOnDataLoss",False)  \
  .load()
```
##  Writing the streaming data from Kafka to Elasticsearch by PySpark.
Please refer to ``` Write_to_ES.py``` for details of how to read the streaming data.
Some of the code used to read the streaming data shown below. 

```python
streamingQuery = (df3.writeStream
                  .format("org.elasticsearch.spark.sql")
                  .outputMode("append")
                  .trigger(processingTime="3 second")
                  .option("numRows", 4)
                  .option("truncate", False)
                  .option("checkpointLocation", checkpoint_dir )
                  .option("es.resource","officeinput3")
                  .option("es.nodes", "localhost:9200")
                  .start())
```
## Elasticsearch index & docs and creation of Kibana Dashboard.

Index names and number of docs can be obtained by running the following command:

```
GET /_cat/indices?v
```

Following graphs have been created using the docs in the Elasticsearch index.

![](C:\Users\oner\Desktop\Kibana_graphs_1.jpg)
![](C:\Users\oner\Desktop\Kibana_graphs_2.jpg)
