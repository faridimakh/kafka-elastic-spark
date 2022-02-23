
------------------------------------------------------------------------------------
curl --silent --show-error -XPUT -H 'Content-Type: application/json' \
http://localhost:9200/_index_template/rmoff_template01/ \
-d'{
"index_patterns": [ "target*" ],
"template": {
"mappings": {"properties":{
        "address": {"type": "text","fields": {"keyword": {"type": "keyword","ignore_above": 256}}},
        "available_bike_stands": {"type": "long"},
        "available_bikes": {"type": "long"},
        "banking": {"type": "boolean"},
        "bike_stands": {"type": "long"},
        "bonus": {"type": "boolean"},
        "contract_name": {"type": "text","fields": {"keyword": {"type": "keyword","ignore_above": 256}}},
        "last_update": {"type": "long"},
        "location": {"type": "geo_point"},
        "name": {"type": "text","fields": {"keyword": {"type": "keyword","ignore_above": 256}}},
        "number": {"type": "long"},
        "status": {"type": "text","fields": {"keyword": {"type": "keyword","ignore_above": 256}}
        }
      }}}}'

---------------------------------------------------------------------
docker exec -it ksqldb ksql http://ksqldb:8088
SET 'auto.offset.reset' = 'earliest';
---------------------------------------------------------------------
CREATE SINK CONNECTOR SINK_ELASTIC WITH (
'connector.class'                     = 'io.confluent.connect.elasticsearch.ElasticsearchSinkConnector',
'connection.url'                      = 'http://elasticsearch:9200',
'value.converter.schema.registry.url' = 'http://schema-registry:8081',
'topics'                              = 'target_topic',
'value.converter'                     = 'io.confluent.connect.avro.AvroConverter',
'key.converter'                       = 'io.confluent.connect.avro.AvroConverter',
'key.converter.schema.registry.url'   = 'http://schema-registry:8081',
'type.name'                           = '_doc',
'key.ignore'                          = 'false',
'schema.ignore'                       = 'true'
);

---------------------------------------------------------------------
