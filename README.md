# Top trending twitter topics microservice - Razvan Vacaru
This readme explains you the following:
- General introduction of the application
- How to build the application
- How to run it
    - Application properties
    - Starting pre configured hadoop in a docker container
    - Starting the application inside or outside a docker container
- Exposed rest api and how to use it
- Clean up docker containers


## Introduction
This is a **Java/Scala** written application, built with **Maven** running on **JVM**. Optionally it can be built into a **Docker** image.
The microservice is a **Spring boot** app which exposes a rest api for analyzing top trending topics per time window interval.
The microservice connects to an **Hdfs** cluster for getting raw data and analyzes the data by connecting to a **Spark** cluster.


## How to build 
From the main folder of the project, where the pom file is, you can run via terminal:
```
#builds the app without tests
$> mvn clean install -DskipTests

#builds the app without tests and then builds a docker image from the built
$> mvn clean install -DskipTests -Ddocker
```

The build can include tests if the property `-DskipTests` is removed from the previous commands.
The docker image of the app can be checked via commands like
 - `docker images` 
 - `docker image inspect gdd-twitter:latest` 
 
 
## How to run 

**Application properties**

The follwing configuration properties are exposed:
- spark.master (default local[*]), location of the spark cluster master
- spark.sql.shuffle.partitions (default 200), number of paritions for shuffled data
- hdfs.default.name, hdfs cluster location 
- topics.hdfs.path.sample (with default), path of the sample json file in hdfs
- topics.hdfs.path.twitter-sample (with default), path of the twitter sample json file in hdfs

Spark and hdfs configuration properties are just a small example, many more properties can be exposed 
and tweaked based on the need, to increase performance of the cluster jobs.

**Starting Hadoop in a docker container**

To start a preconfigured and ready to use hadoop docker container follow the instructions in `./hadoop/README.md`.
This will start a local hadoop with the sample files already placed in the correct path in hdfs.

**Starting the application locally**

Run the application locally
 - via an IDE, by running the class `GddTwitterApplication`
 - via terminal, by running from the project folder `>$ java -jar ./target/gdd-twitter-0.0.1-SNAPSHOT.jar`
 
There is no need to change any property in the properties file `./gdd-twitter-raz/src/main/resources/application.properties` 
in order to be able to load the data sample files.
 
**Starting the application in a docker container(not working)**
 
The app can run in a docker container as well, tho due to some network misconfiguration the app cannot 
connect to the hdfs datanode at port 50010. I provide both hadoop and app containers with the `--network gdd-net` 
option so the containers run on the same network, the app can indeed connect to the master `hdfs://hdfs-local:8020` but later 
fails to actually load the sample files from the datanode on port 50010. I didn't have time to fix this.

To run the app in docker the following application properties need to be provided:
```
hdfs.default.name=hdfs://hdfs-local:8020
topics.hdfs.path.sample=hdfs://hdfs-local:8020/user/gdd/sample.json
topics.hdfs.path.twitter-sample=hdfs://hdfs-local:8020/user/gdd/twitter-sample.json
```
So for running the app in a docker container, just for fun, run via terminal:

`>$ docker run --name gdd-trending-twitter-raz --network gdd-net -p 8080:8080 gdd-twitter`
 
 
## Rest Api
First of all a healthcheck endpoint is exposed in order to manage a cluster of app instances externally, 
together with additional app metrics and alerts:
- localhost:8080/healthcheck GET

A rest api for analyzing the top N trending topics per window time interval is available:
- localhost:8080/api/trending_topics?noTopics=5&windowPhrase=1 day

The request parameters `?noTopics=5&windowPhrase=1 day`
 - can be omitted as default values 5 and 1 day will be implied.
 - have backend validation

Window phrase parameter accepts words like:
- `second`
- `minute`
- `hour`
- `day` 
- `week`

## Clean up the docker containers
To stop and remove the docker containers run
 - `docker rm -f hdfs-local`
 - `docker rm -f gdd-trending-twitter-raz`
