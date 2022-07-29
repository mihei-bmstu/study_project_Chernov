package testChernov.labs

import org.apache.spark.sql.SparkSession
import testChernov.system._
import org.apache.spark.util.AccumulatorV2


case class Result(uniqProducts: Set[Int], uniqNumOfProducts: Seq[Int], sumNumOfProduct: Int)

case class Lab6(spark: SparkSession) {
  def lab6AggregateBy(): Unit ={
    println("Lab 6 started")
    import spark.implicits._

    val orderRDD = spark.read
      .option("delimiter", "\t")
      .schema(Parameters.orderSchema)
      .csv(Parameters.path_order)
      .as[Order]
      .rdd
      .filter(_.Status == "delivered")
      .map{ case Order(custID, orderID, prodID, numberProd, orderDate, status) =>
        (custID, (prodID, numberProd))}

    val distinctProdID = orderRDD.map{ case (custID, (prodID, numberProd)) =>
      (prodID)}
      .distinct()
      .count()
    println(s"Distinct product id: $distinctProdID")

    val maxNumberProducts = orderRDD.map{ case (custID, (prodID, numberProd)) =>
      (numberProd)}
      .max()
    println(s"Max number of products: $maxNumberProducts")

    val minNumberProducts = orderRDD.map{ case (custID, (prodID, numberProd)) =>
      (numberProd)}
      .min()
    println(s"Max number of products: $minNumberProducts")

    val sumNumberProducts = orderRDD.map{ case (custID, (prodID, numberProd)) =>
      (numberProd)}
      .reduce(_ + _)
    println(s"Sum number of products: $sumNumberProducts")

    println("Lab 6 finished")
  }
}
