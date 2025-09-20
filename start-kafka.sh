#!/bin/bash

# Create a network for the containers
docker network create kafka-net || true

# Start Zookeeper
docker run -d \
  --name zoo1 \
  --hostname zoo1 \
  --network kafka-net \
  -p 2181:2181 \
  -e ZOOKEEPER_CLIENT_PORT=2181 \
  -e ZOOKEEPER_SERVER_ID=1 \
  -e ZOOKEEPER_SERVERS=zoo1:2888:3888 \
  confluentinc/cp-zookeeper:7.6.1

# Start Kafka broker 1
docker run -d \
  --name kafka1 \
  --hostname kafka1 \
  --network kafka-net \
  -p 9092:9092 \
  -p 29092:29092 \
  -e KAFKA_LOG_RETENTION_MS=-1 \
  -e KAFKA_LOG_RETENTION_HOURS=-1 \
  -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=true \
  -e KAFKA_DEFAULT_REPLICATION_FACTOR=2 \
  -e KAFKA_NUM_PARTITIONS=2 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=2 \
  -e KAFKA_ADVERTISED_LISTENERS=INTERNAL://kafka1:19092,EXTERNAL://${DOCKER_HOST_IP:-127.0.0.1}:9092,DOCKER://host.docker.internal:29092 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,DOCKER:PLAINTEXT \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL \
  -e KAFKA_ZOOKEEPER_CONNECT=zoo1:2181 \
  -e KAFKA_BROKER_ID=1 \
  -e KAFKA_LOG4J_LOGGERS="kafka.controller=INFO,kafka.producer.async.DefaultEventHandler=INFO,state.change.logger=INFO" \
  -e KAFKA_AUTHORIZER_CLASS_NAME=kafka.security.authorizer.AclAuthorizer \
  -e KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND=true \
  confluentinc/cp-kafka:7.6.1

# Start Kafka broker 2
docker run -d \
  --name kafka2 \
  --hostname kafka2 \
  --network kafka-net \
  -p 9093:9093 \
  -p 29093:29093 \
  -e KAFKA_LOG_RETENTION_MS=-1 \
  -e KAFKA_LOG_RETENTION_HOURS=-1 \
  -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=true \
  -e KAFKA_DEFAULT_REPLICATION_FACTOR=2 \
  -e KAFKA_NUM_PARTITIONS=2 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=2 \
  -e KAFKA_ADVERTISED_LISTENERS=INTERNAL://kafka2:19093,EXTERNAL://${DOCKER_HOST_IP:-127.0.0.1}:9093,DOCKER://host.docker.internal:29093 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,DOCKER:PLAINTEXT \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL \
  -e KAFKA_ZOOKEEPER_CONNECT=zoo1:2181 \
  -e KAFKA_BROKER_ID=2 \
  -e KAFKA_LOG4J_LOGGERS="kafka.controller=INFO,kafka.producer.async.DefaultEventHandler=INFO,state.change.logger=INFO" \
  -e KAFKA_AUTHORIZER_CLASS_NAME=kafka.security.authorizer.AclAuthorizer \
  -e KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND=true \
  confluentinc/cp-kafka:7.6.1
