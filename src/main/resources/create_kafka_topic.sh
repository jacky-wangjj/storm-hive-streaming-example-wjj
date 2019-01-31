#!/bin/bash
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --zookeeper 10.110.181.41:2181 --replication-factor 1 --partitions 1 --topic stock_topic