package packfar


import scala.language.postfixOps
import sys.process._

object producervilib extends App {

  //  -----------------------------------------------------------------------------------------------------
//  "./sendConnector_and_shema.sh" !!
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
  }


}

