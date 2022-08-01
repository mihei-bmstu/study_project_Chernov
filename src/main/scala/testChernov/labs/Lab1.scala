package testChernov.labs

import org.apache.spark.sql.SparkSession
import testChernov.system._

case class Lab1 (spark: SparkSession) {
  def lab1ReduceByKey(): Unit = {
    println("Lab 1 started")
    import spark.implicits._

    val rddFromFile = spark.read
      .option("delimiter", "\t")
      .schema(Parameters.orderSchema)
      .csv(Parameters.path_order)
      .as[Order]
      .rdd

    val rddFiltered = rddFromFile
      .filter(_.Status == "delivered")
      .map { order =>
        (order.Customer_ID, (order.Number_Of_Products, 1))
      }
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)) //аналогично

    println("Client ID, Total products, Number of orders")
    rddFiltered.foreach(println)
    println("Lab 1 finished")
  }
}
