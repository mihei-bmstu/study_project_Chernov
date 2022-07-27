package testChernov.labs

import org.apache.spark.rdd._
import org.apache.spark.sql.SparkSession
import testChernov.system._
import java.sql.Date

case class Order(customerID: Int,
                 orderID: Int,
                 productID: Int,
                 numberOfProduct: Int,
                 orderDate: Date,
                 status: String)

case class OrderShort(customerID: Int, numberOfProduct: Int)

class Lab1 (spark: SparkSession) {
  def lab1ReduceByKey(): Unit = {
    println("Lab 1 started")
    val rddFromFile = spark.read
      .option("delimiter", "\t")
      .csv(Parameters.path_order)
      .rdd

    val rddFiltered = rddFromFile
      .filter(_(5) == "delivered")
      .map(x=>Order(x(0).asInstanceOf[Int],
        x(1).asInstanceOf[Int],
        x(2).asInstanceOf[Int],
        x(3).asInstanceOf[Int],
        x(4).asInstanceOf[Date],
        x(5).asInstanceOf[String])
      )
      .map { case Order(x1, x2, x3, x4, x5, x6) => OrderShort(x1, x4) }

    rddFiltered.foreach(println)

  }
}

