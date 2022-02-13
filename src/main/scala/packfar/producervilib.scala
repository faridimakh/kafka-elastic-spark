package packfar

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.util.Properties

object producervilib extends App {

  val url = "https://api.jcdecaux.com/vls/v1/stations?apiKey=2a5d13ea313bf8dc325f8783f888de4eb96a8c14"
  //  -----------------------------------------------------------------------------------------------------
  val srClient = new CachedSchemaRegistryClient("http://localhost:8081", 1)
  val vilibSchema = srClient.getByID(srClient.getSchemaMetadata("vilib", 1).getId)
  val vilibSchema_pos = srClient.getByID(srClient.getSchemaMetadata("vilibpos", 1).getId)

  val spark = new SparkSession.Builder()
    .appName("kafka-client")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  //  -----------------------------------------------------------------------------------------------------
  def GetUrlContentJson(url: String): DataFrame = {
    val result = scala.io.Source.fromURL(url).mkString
    val jsonResponseOneLine = result.stripLineEnd
    val jsonRdd = spark.sparkContext.parallelize(jsonResponseOneLine :: Nil)
    val jsonDf = spark.read.json(jsonRdd)
    jsonDf
  }

  def getpos(s: String, i: Int): Double = {
    s.drop(1).dropRight(1).split(",")(i).toDouble}
    //  -----------------------------------------------------------------------------------------------------
  while (true){
    val vilibDF = GetUrlContentJson(url)

    //  -----------------------------------------------------------------------------------------------------

    val vilibrdd: Array[Row] = vilibDF.rdd.collect()

    val avrovilib: List[GenericRecord] = vilibrdd.map({ row =>
      new GenericRecordBuilder(vilibSchema)
        .set("address", row(0).toString)
        .set("available_bike_stands", row(1).toString.toLong)
        .set("available_bikes", row(2).toString.toLong)
        .set("banking", row(3).toString.toBoolean)
        .set("bike_stands", row(4).toString.toLong)
        .set("bonus", row(5).toString.toBoolean)
        .set("contract_name", row(6).toString)
        .set("last_update", row(7).toString.toLong)
        .set("name", row(8).toString)
        .set("number", row(9).toString.toLong)
        .set("position",
          new GenericRecordBuilder(vilibSchema_pos)
            .set("lat", getpos(row(10).toString, 0))
            .set("lng", getpos(row(10).toString, 1))
            .build())
        .set("status", row(11).toString)
        .build()
    }).toList


    val producerProperties = new Properties()
    producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getName)
    producerProperties.setProperty("schema.registry.url", "http://localhost:8081")

    val producer = new KafkaProducer[String, GenericRecord](producerProperties)
    avrovilib.map(avroMessage => new ProducerRecord[String, GenericRecord]("vilib", avroMessage.get("name").toString, avroMessage))
      .map(producer.send)
    producer.flush()


  }

  }
