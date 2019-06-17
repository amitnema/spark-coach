[![Build Status](https://travis-ci.org/amitnema/spark-coach.svg?branch=master)](https://travis-ci.org/amitnema/spark-coach)  |  [![Quality Gate](https://sonarcloud.io/api/project_badges/measure?project=org.apn.spark%3Aspark-coach&metric=alert_status)](https://sonarcloud.io/dashboard?id=org.apn.spark%3Aspark-coach)  |  [![Sputnik](https://sputnik.ci/conf/badge)](https://sputnik.ci/app#/builds/amitnema/spark-coach)

# spark-coach
This project contains the learning and experiments with the [Apache Spark](https://spark.apache.org/ "Lightning-fast unified analytics engine").

Following packages contains the relative api program.
* pacakage _org.apn.spark.rdd_ contains _Spark RDD_ code.
* pacakage _org.apn.spark.dsl_ contains _Spark DSL_ code.
* pacakage _org.apn.spark.sql_ contains _Spark SQL_ code.
* pacakage _org.apn.spark.sql.streaming_ contains _Spark Structured Streaming_ code.
* pacakage _org.apn.spark.streaming_ contains __Spark Streaming_ (DStream) code.

---
# Quick Start

## Prerequisites

The Spark-Coach uses Java 1.8 features so you'll need the correct jdk to run it.

Some of the features of Kafka used in these are only available since the 0.10.x release.

## Setup local environment 

The master branch of this demo uses 0.10.x features of [Apache Kafka](https://kafka.apache.org/ "A distributed streaming platform") so all you need to do is clone and install kafka 
0.10.0.0 into your local maven :
 
    $ git clone https://github.com/apache/kafka.git $KAFKA_HOME
    $ cd $KAFKA_HOME
    $ git checkout 0.10.0.0
    $ gradle
    $ ./gradlew -PscalaVersion=2.11 jar 
    $
    $ # IF YOU ARE USING WINDOWS, USE `.bat` IN PLACE OF `.sh` FOR THE LAUNCH SCRIPTS BELLOW:
    $ export SCALA_VERSION="2.11.8"; export SCALA_BINARY_VERSION="2.11";
    $ ./bin/zookeeper-server-start.sh ./config/zookeeper.properties &
    $ ./bin/kafka-server-start.sh ./config/server.properties

Initialize topics: if you're already running a local Zookeeper and Kafka and you have topic auto-create enabled on the 
broker you can skip the following setup, just note that if your default partitions number is 1 you will only be able 
to run a single instance demo.

    $ # IF YOU ARE USING WINDOWS, USE `.bat` IN PLACE OF `.sh` FOR THE LAUNCH SCRIPTS BELLOW:
    $ ./bin/kafka-topics.sh --zookeeper localhost --create --topic topicname --replication-factor 1 --partitions 4
 
## Build the Spark-Coach

In another terminal `cd` into the directory where you have cloned the `spark-coach` project and use provided
maven to create archive.

    $ mvn clean package

