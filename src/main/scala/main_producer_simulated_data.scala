import packfar.{send_df_to_kafka, simulate_data}
import scala.language.postfixOps
import scala.sys.process._

object main_producer_simulated_data {
  def main(args: Array[String]): Unit = {
      //  -----------------------------------------------------------------------------------------------------
      "./sendConnector_and_shema.sh" !!

    println("connector created")
      //  -----------------------------------------------------------------------------------------------------
      Thread.sleep(5000)
      //  -----------------------------------------------------------------------------------------------------
    while (true) {
      val df1 = simulate_data()
      send_df_to_kafka(df1)
      Thread.sleep(3000)
      //  -----------------------------------------------------------------------------------------------------
    }
  }

}
