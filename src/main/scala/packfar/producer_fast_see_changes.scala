package packfar
object producer_fast_see_changes extends App {
  while (true) {
    val df1 = simulate_data()
    send_df_to_kafka(df1)
    Thread.sleep(1000)
  }
}



