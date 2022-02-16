package packfar

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object test_far2 extends App {
  val url = "https://api.jcdecaux.com/vls/v1/stations?apiKey=2a5d13ea313bf8dc325f8783f888de4eb96a8c14"
  //  -----------------------------------------------------------------------------------------------------


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
    s.drop(1).dropRight(1).split(",")(i).toDouble
  }
  //  -----------------------------------------------------------------------------------------------------

    val vilibDF = GetUrlContentJson(url).where("not(address!='' and name=='STORTORGET')")

    //  -----------------------------------------------------------------------------------------------------

//    val vilibrdd: Array[Row] = vilibDF.rdd.collect()
//    vilibrdd.map(r=>r(10).toString.drop(1).dropRight(1)).take(10).foreach(println)

vilibDF.show(5,truncate = false)
}
