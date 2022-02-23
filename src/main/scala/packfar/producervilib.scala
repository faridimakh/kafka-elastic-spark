package packfar

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.util.Properties

object producervilib extends App {


  //  -----------------------------------------------------------------------------------------------------
  val srClient = new CachedSchemaRegistryClient("http://localhost:8081", 1)
  val vilibSchema = srClient.getByID(srClient.getSchemaMetadata("target_topic", 1).getId)
  val vilibSchema_pos = srClient.getByID(srClient.getSchemaMetadata("vilib_position", 1).getId)

  def getpos(s: String, i: Int): Double = {
    s.drop(1).dropRight(1).split(",")(i).toDouble
  }
  //  -----------------------------------------------------------------------------------------------------

  while (true) {
    val vilibrdd = GetUrlContentJson(url).where("not(address!='' and name=='STORTORGET')").rdd.collect()
    //  -----------------------------------------------------------------------------------------------------
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
        .set("location",
          new GenericRecordBuilder(vilibSchema_pos)
            .set("lat", getpos(row(10).toString, 0))
            .set("lon", getpos(row(10).toString, 1))
            .build())
        .set("status", row(11).toString)
        .build()
    }).toList

    val producerProperties = new Properties()
    producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getName)
    producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getName)
    producerProperties.setProperty("schema.registry.url", "http://localhost:8081")

    val producer = new KafkaProducer[String, GenericRecord](producerProperties)
    avrovilib.map(avroMessage => new ProducerRecord[String, GenericRecord](target_topic, avroMessage.get("name").toString, avroMessage))
      .map(producer.send)
    producer.flush()
  }

  //  }

}

