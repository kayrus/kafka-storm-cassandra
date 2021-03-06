# Overview

This is an example repo that shows how to set up a Java project to use the `KafkaSpout` that is now part of Storm with version 0.9.3.  Other examples online at the time of this writing are either wrong (outdated) or show how to run with KafkaSpout in local mode but not "cluster-mode" against a separate Kafka system or confusing in that they add a lot of superfluous stuff (and do the example in Scala).  This example is in the vein of "do the simplest thing that can work".

Getting the pom set up right so that the incompatible logging libraries between Storm and Kafka don't shoot the topology in the head is the hardest part (lost a day of my life getting that sorted out).  So that is probably the most fragile piece - it may break on other systems and with other versions (past or future).

The topologies are a basic wordcount topology, based on Chapter 1 of P. Taylor Goetz's [Storm Blueprints](https://www.packtpub.com/big-data-and-business-intelligence/storm-blueprints-patterns-distributed-real-time-computation) book.

* Demo uses a Kafka `sentence` Topic as the source of sentences
* Demo uses anchors and acks tuples ("reliable")


# Tested against

This code has been tested on Linux in cluster-mode using:

* Storm 0.9.3
* Kafka 2.10-0.8.2.0
* the version of Zookeeper that comes with kafka_2.10-0.8.2.0 (zookeeper-3.4.5)


# Usage

The KafkaSpout based topologies can be run in local mode.


## Running the KafkaSpout based wordcount example

Install and start ZooKeeper, Storm and Kafka (see version notes above).  This example assumes all are running on 172.17.8.101.  If not, you will need to change the `zkHostPort` settings in the kafka `WordCountAckedTopology` and `WordCountNonAckedTopology` classes to point to where zookeeper is running.

Create the "sentences" topic in Kafka:

    $KAFKA_BIN/kafka-topics.sh --create --zookeeper 172.17.8.101:2181 --replication-factor 1 --partitions 1 --topic topic

The `pom.xml` can be adjusted to specify which version of Kafka you want to depend on.  The default is 2.10-0.8.2.0.

Compile and build the uber-jar:

    mvn clean package

Submit the uber-jar to storm and specify either the Acked:

    storm jar target/kafka-storm-cassandra-1.0-SNAPSHOT-jar-with-dependencies.jar kayrus.kafkacassandra.kafka.KafkaCassandraAckedTopology

Now you'll need to put some sentences into the sentence topic.

You can either do it manually with:

    $KAFKA_BIN/kafka-console-producer.sh --broker-list 172.17.8.101:9092 --topic topic
    (and now type a bunch of sentences in)

or use the example code I provide that puts in a bunch of sentences over a few minutes.

    java -cp target/kafka-storm-cassandra-1.0-SNAPSHOT-jar-with-dependencies.jar kayrus.kafkacassandra.kafka.PopulateKafkaSentenceTopic


If you have the Storm UI launched (`storm ui`) you can go to [http://172.17.8.101:8080](http://172.17.8.101:8080) and watch the progress of the topology



