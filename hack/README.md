Alluvial Hack
=============

## I. Development environment
```bash
cd hack/

# bootstrap kafka, debezium (kafka-connect) and other services 
docker compose up -d

# Create debezium's mysql.sakila connector
curl -i -X POST http://localhost:8083/connectors/ \
    -H "Accept:application/json" \
    -H "Content-Type:application/json" \
    -d @debezium/mysql-sakila.json
    
# Create debezium's postgres.dvdrental connector
curl -i -X POST http://localhost:8083/connectors/ \
    -H "Accept:application/json" \
    -H "Content-Type:application/json" \
    -d @debezium/postgres-dvdrental.json
    
# Update existing connector
jq ".config" debezium/mysql-sakila.json | \
curl -i -X PUT http://localhost:8083/connectors/demo.debezium.mysql.sakila/config \
    -H "Accept:application/json" \
    -H "Content-Type:application/json" \
    -d @-
```

Then add follow lines to the `/etc/hosts` file
```
10.5.0.3 kafka
10.5.0.4 schema-registry
```

## II. Incremental snapshot
### Signalling Table
```sql
CREATE TABLE `debezium_signal` (
  `id` varchar(42) PRIMARY KEY,
  `type` varchar(32) NOT NULL,
  `data` varchar(2048) NULL
)

INSERT INTO debezium_signal (`id`, `type`, `data`)
VALUES (uuid(), 'log', '{"message": "Signal message at offset {}"}');

INSERT INTO debezium_signal (`id`, `type`, `data`)
VALUES (uuid(), 'execute-snapshot', '{"data-collections": ["sakila.actor"], "type":"incremental"}');
```

### Signalling Topic (Read-only mode)
1. create topic
    ```bash
    kafka-topics.sh --bootstrap-server=kafka:9092 \
      --create --topic=_debezium.signals \
      --config=cleanup.policy=delete \
      --partitions=1
    ```
2. create connector. NOTE: following config is required
    ```properties
    signal.kafka.bootstrap.servers=kafka:9092
    signal.kafka.topic=_debezium.signals
    read.only=true
    ```
3. publish a signal
    ```bash
    kafka-console-producer.sh --bootstrap-server=kafka:9092 \
        --property "key.serializer=org.apache.kafka.common.serialization.StringSerializer" \
        --property "value.serializer=org.apache.kafka.connect.json.JsonSerializer" \
        --property "key.separator=;" \
        --topic _debezium.signals
    
    > debezium.mysql.sakila;{"type":"execute-snapshot","data": {"data-collections": ["sakila.actor"], "type": "INCREMENTAL"}}
    ```
