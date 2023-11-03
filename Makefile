KAFKA_PATH="/opt/kafka"

zookeeper:
	@$(KAFKA_PATH)/bin/zookeeper-server-start.sh config/zookeeper.properties

broker:
	@$(KAFKA_PATH)/bin/kafka-server-start.sh config/server.properties

list:
	@$(KAFKA_PATH)/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

create:
	@$(KAFKA_PATH)/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic $(topic)

delete:
	@$(KAFKA_PATH)/bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic $(topic)

clean:
	@rm -rf d1/*
	@rm -rf d2/*
	@rm -rf d3/*
	@rm -rf d4/*