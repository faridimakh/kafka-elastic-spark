import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.sql.functions.{rand, round}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties
import scala.io.Source

package object packfar {
  //  -----------------------------------------------------------------------------------------------------
  lazy val vilibSchema: Schema = new Schema.Parser().parse(Source.fromFile("src/main/resources/vilib.avsc").mkString)
  lazy val vilibSchema_pos: Schema = new Schema.Parser().parse(Source.fromFile("src/main/resources/position.avsc").mkString)
  val target_topic = "target_topic"
  val vilib_group_consumers = "vilib-group_consumers"
  val vilib_position = "vilib_position"
  lazy val posSchema: Schema = new Schema.Parser().parse(Source.fromFile("src/main/resources/position.avsc").mkString)
  lazy val srClient = new CachedSchemaRegistryClient("http://localhost:8081", 1)
  lazy val key_vilib = "2a5d13ea313bf8dc325f8783f888de4eb96a8c14"
  lazy val spark: SparkSession = new SparkSession.Builder()
    .appName("kafka-client")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  lazy val url: String = "https://api.jcdecaux.com/vls/v1/stations?apiKey=" + key_vilib

  //  -----------------------------------------------------------------------------------------------------

  //  -----------------------------------------------------------------------------------------------------
  def GetUrlContentJson(url: String): DataFrame = {
    val result = scala.io.Source.fromURL(url).mkString
    val jsonResponseOneLine = result.stripLineEnd
    val jsonRdd = spark.sparkContext.parallelize(jsonResponseOneLine :: Nil)
    val jsonDf = spark.read.json(jsonRdd)
    jsonDf
  }

  def getpos(s: String, i: Int): Double = {
    s.drop(1).dropRight(1).split(",")(i).toDouble
  }

  def send_df_to_kafka(df: DataFrame): Unit = {
    val avrovilib: List[GenericRecord] = df.rdd.collect().map({ row =>
      new GenericRecordBuilder(vilibSchema)
        .set("address", row(0).toString)
        .set("available_bike_stands", row(1).toString.toLong)
        .set("available_bikes", row(2).toString.toLong)
        .set("banking", row(3).toString.toBoolean)
        .set("bike_stands", row(4).toString.toLong)
        .set("bonus", row(5).toString.toBoolean)
        .set("contract_name", row(6).toString)
        //          .set("last_update", row(7).toString.toLong)
        .set("name", row(8).toString)
        .set("number", row(9).toString.toLong)
        .set("location",
          new GenericRecordBuilder(vilibSchema_pos)
            .set("lat", getpos(row(10).toString, 0))
            .set("lon", getpos(row(10).toString, 1))
            .build())
        .set("status", row(11).toString)
        .build()
    }).toList
    //  -----------------------------------------------------------------------------------------------------
    val producerProperties = new Properties()
    producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getName)
    producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getName)
    producerProperties.setProperty("schema.registry.url", "http://localhost:8081")

    val producer = new KafkaProducer[String, GenericRecord](producerProperties)
    avrovilib.map(avroMessage => new ProducerRecord[String, GenericRecord](target_topic, avroMessage.get("name").toString, avroMessage))
      .map(x => producer.send(x).get())
    producer.flush()
  }


  def simulate_data(): DataFrame = {
    var df1 = GetUrlContentJson(url).where("not(address!='' and name=='STORTORGET')")
    df1 = df1.withColumn("available_bike_stands", round((rand() * 5 + 5) / 2, 0).cast(IntegerType))
    df1 = df1.withColumn("available_bikes", round(rand() * 5 + 10, 0).cast(IntegerType))
    df1 = df1.withColumn("bike_stands", df1("available_bikes") + df1("available_bike_stands"))
    df1.toDF()
  }

  //  -----------------------------------------------------------------------------------------------------
}
