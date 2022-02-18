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
    -d @res/mysq-sakila.json
```
