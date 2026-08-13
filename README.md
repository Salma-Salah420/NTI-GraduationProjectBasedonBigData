# NTI-GraduationProjectBasedonBigData

docker exec maritime-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic ais-live \
  --partitions 3 \
  --replication-factor 1

docker exec -it maritime-spark-master \
    /opt/spark-apps/run_kafka_test.sh

sudo chown -R 50000:0 airflow