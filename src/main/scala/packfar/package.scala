import org.apache.spark.sql.{DataFrame, SparkSession}

package object packfar {
  //  -----------------------------------------------------------------------------------------------------
  val spark: SparkSession = new SparkSession.Builder()
    .appName("kafka-client")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  //  -----------------------------------------------------------------------------------------------------
  val url: String = "https://api.jcdecaux.com/vls/v1/stations?apiKey=2a5d13ea313bf8dc325f8783f888de4eb96a8c14"

  //  -----------------------------------------------------------------------------------------------------
  def GetUrlContentJson(url: String): DataFrame = {
    val result = scala.io.Source.fromURL(url).mkString
    val jsonResponseOneLine = result.stripLineEnd
    val jsonRdd = spark.sparkContext.parallelize(jsonResponseOneLine :: Nil)
    val jsonDf = spark.read.json(jsonRdd)
    jsonDf
  }

  //  -----------------------------------------------------------------------------------------------------
  val target_topic = "target_topic"
  //  -----------------------------------------------------------------------------------------------------
  val vilib_group_consumers = "vilib-group_consumers"
  val vilib_position = "vilib_position"
}
