####SERVICES
# start hadoop
start-all.sh

# start hbase with zookeeper
start-hbase.sh

# start kafka (zookeeper is already starts with hbase)
# zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties #OPTIONAL you can use it if zookeeper is no running
kafka-server-start.sh $KAFKA_HOME/config/server.properties

#check running application
jps
ps -ef | grep zookeeper

#####CONSUMERS
#create kafka topics
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic twitter_topic
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic mentions_topic
#check kafka topics
kafka-topics.sh --list --zookeeper localhost:2181

#start hashtag counter 
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 kafkaCountHashtags.py localhost 9092 twitter_topic

#start kafka consumer
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic mentions_topic


####PRODUCERS
#start split mentions producer
python3 kafkaMentionsProducer.py localhost 9092 twitter_topic mentions_topic

#start tweet producer 
python3 kafkaTweetProducer.py localhost 9092 twitter_topic bitcoin,crypto,NFT



#####TESTING PURPOSES
#starting kafka producer
kafka-console-producer.sh --broker-list localhost:9092 --topic twitter_topic