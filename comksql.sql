CREATE SINK CONNECTOR SINK_ELASTIC_vilib WITH (
  'connector.class'                     = 'io.confluent.connect.elasticsearch.ElasticsearchSinkConnector',
  'connection.url'                      = 'http://elasticsearch:9200',
  'value.converter'                     = 'io.confluent.connect.avro.AvroConverter',
  'value.converter.schema.registry.url' = 'http://schema-registry:8081',
  'key.converter'                       = 'io.confluent.connect.avro.AvroConverter',
  'key.converter.schema.registry.url'   = 'http://schema-registry:8081',
  'type.name'                           = '_doc',
  'topics'                              = 'vilib',
  'key.ignore'                          = 'false',
  'schema.ignore'                       = 'false'
);

------------------------------------------------------------------
------------------------------------------------------------------
------------------------------------------------------------------
-----------------connector updating index-------------------
------------------------------------------------------------------
------------------------------------------------------------------
docker exec -it ksqldb ksql http://ksqldb:8088
SET 'auto.offset.reset' = 'earliest';
create stream vilibstream with (KAFKA_TOPIC='vilib', VALUE_FORMAT='avro');


CREATE STREAM TARGET_STREAM WITH (KAFKA_TOPIC='target_topic') AS
        SELECT *,
        STRUCT("lat" := LOCATION->LAT, "lon":= LOCATION->LON) AS "location_example_01",
        CAST(LOCATION->LAT AS VARCHAR)  + ',' + CAST(LOCATION->LON AS VARCHAR) AS "location_example_02"
        .FROM vilibstream;
