package testChernov.labs

import org.apache.spark.sql.SparkSession
import testChernov.system._

case class Order(Customer_ID: String,
                 Order_ID: Integer,
                 Product_ID: Integer,
                 Number_Of_Products: Integer,
                 Order_Date: String,
                 Status: String)

class Lab1 (spark: SparkSession) {
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
      .map { case Order(x1, x2, x3, x4, x5, x6) => (x1, (x4, 1)) }
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    rddFiltered.foreach(println)
    println("Lab 1 finished")
  }
}
