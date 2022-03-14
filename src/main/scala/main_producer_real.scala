import packfar.{GetUrlContentJson, send_df_to_kafka, url}

import scala.language.postfixOps
import scala.sys.process._

object main_producer_real {
  def main(args: Array[String]): Unit = {


    //  -----------------------------------------------------------------------------------------------------
    "./sendConnector_and_shema.sh" !!
    //  -----------------------------------------------------------------------------------------------------

    //  -----------------------------------------------------------------------------------------------------
    Thread.sleep(5000)
    var df1 = GetUrlContentJson(url).where("not(address!='' and name=='STORTORGET')")
    send_df_to_kafka(df1)
    while (true) {
      Thread.sleep(10000)
      val df2 = GetUrlContentJson(url).where("not(address!='' and name=='STORTORGET')")
      val df3 = df2.except(df1)
      if (df3.toDF().persist().count() != 0) {
        df3.show()
        send_df_to_kafka(df3)
      }
      df1 = df2
      //  -----------------------------------------------------------------------------------------------------

    }
  }


}
