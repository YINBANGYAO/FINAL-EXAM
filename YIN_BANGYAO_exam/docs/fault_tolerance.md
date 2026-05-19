# Kafka Fault Tolerance Evidence

## Test setup

The cluster was started with:

```bash
docker compose up -d
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --create --if-not-exists --topic sensor-events --partitions 3 --replication-factor 3
docker exec kafka1 kafka-configs --bootstrap-server kafka1:29092 --alter --entity-type topics --entity-name sensor-events --add-config min.insync.replicas=2
```

## Before failure

```text
Topic: sensor-events PartitionCount: 3 ReplicationFactor: 3 Configs: min.insync.replicas=2
  Partition: 0 Leader: 1 Replicas: 1,2,3 Isr: 1,2,3
  Partition: 1 Leader: 2 Replicas: 2,3,1 Isr: 2,3,1
  Partition: 2 Leader: 3 Replicas: 3,1,2 Isr: 3,1,2
```

Leader distribution was balanced across the three brokers: partition 0 on broker 1, partition 1 on broker 2 and partition 2 on broker 3.

## Broker failure

I stopped broker `kafka2`:

```bash
docker stop kafka2
```

## After stopping kafka2

```text
Topic: sensor-events PartitionCount: 3 ReplicationFactor: 3 Configs: min.insync.replicas=2
  Partition: 0 Leader: 1 Replicas: 1,2,3 Isr: 1,3
  Partition: 1 Leader: 3 Replicas: 2,3,1 Isr: 3,1
  Partition: 2 Leader: 3 Replicas: 3,1,2 Isr: 3,1
```

Partition 1 originally had broker 2 as leader. After the broker was stopped, Kafka elected broker 3 as the new leader and kept brokers 3 and 1 in the ISR. Because `min.insync.replicas=2`, the topic can still accept writes from producers configured with `acks=all` while one broker is unavailable.

## Recovery

After restarting `kafka2`, the broker rejoined the ISR:

```text
Topic: sensor-events PartitionCount: 3 ReplicationFactor: 3 Configs: min.insync.replicas=2
  Partition: 0 Leader: 1 Replicas: 1,2,3 Isr: 1,3,2
  Partition: 1 Leader: 3 Replicas: 2,3,1 Isr: 3,1,2
  Partition: 2 Leader: 3 Replicas: 3,1,2 Isr: 3,1,2
```

The test demonstrates leader re-election and recovery of the in-sync replica set.
