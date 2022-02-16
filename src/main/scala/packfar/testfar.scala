package packfar

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

object testfar extends App {
  // 1. Create Schema Registry client
  val srClient = new CachedSchemaRegistryClient("http://localhost:8081", 1)
  // 2. Get the list of registered subjects
  srClient.getAllSubjects.asScala.foreach(println)
//  val metadata = srClient.getSchemaMetadata("test01-key", 1)
//  val schemaId = metadata.getId
//  val schema = srClient.getByID(schemaId)
//  println(schema.toString(true))
}