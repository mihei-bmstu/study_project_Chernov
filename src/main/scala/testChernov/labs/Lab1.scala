package testChernov.labs

import org.apache.spark.rdd._
import org.apache.spark.sql.SparkSession

class Lab1 (spark: SparkSession) {
  def lab1ReduceByKey(): Unit = {
    println("Hello")
    val rddFromFile = spark.read.option("delimiter", "\t").csv("./dataset/order/order.csv").rdd
    val rddFiltered = rddFromFile
      .filter(_(5) == "delivered")

    rddFiltered.foreach(println)

  }
}

