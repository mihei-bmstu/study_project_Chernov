package testChernov.labs

import org.apache.spark.sql.SparkSession
import testChernov.system._



case class Result(uniqProducts: Set[Int], uniqNumOfProducts: Seq[Int], sumNumOfProduct: Int)

case class Lab6(spark: SparkSession) {
  def lab6AggregateBy(): Unit = {
    println("Lab 6 started")
    import spark.implicits._

    val orderRDD = spark.read
      .option("delimiter", "\t")
      .schema(Parameters.orderSchema)
      .csv(Parameters.path_order)
      .as[Order]
      .rdd
      .filter(_.Status == "delivered")
      .map { case Order(custID, orderID, prodID, numberProd, orderDate, status) =>
        (custID, (prodID, numberProd))
      }

    val aggregateOrder = (acc: Result, prodWithNumber: (Integer, Integer)) => {
      val (prodId, prodNumber) = prodWithNumber
      Result(
        uniqProducts = acc.uniqProducts + prodId,
        uniqNumOfProducts = acc.uniqNumOfProducts :+ prodNumber.toInt,
        sumNumOfProduct = acc.sumNumOfProduct + prodNumber
      )
    }

    val aggregated = orderRDD.aggregateByKey(Result(Set.empty[Int], Seq.empty[Int], 0))(aggregateOrder, (acc1, acc2) => {
      Result(
        acc1.uniqProducts ++ acc2.uniqProducts,
        acc1.uniqNumOfProducts ++ acc2.uniqNumOfProducts,
        acc1.sumNumOfProduct + acc2.sumNumOfProduct
      )
    })

    aggregated.mapValues { case Result(uniqProducts, uniqNumOfProducts, sumNumOfProduct) =>
      Seq(
        uniqProducts.size,
        uniqNumOfProducts.max,
        uniqNumOfProducts.min,
        sumNumOfProduct
      ).mkString("\t")
    }.map { case (k, v) => s"$k\t$v" }.saveAsTextFile("./output/lab6")



    /*  val distinctProdID = orderRDD.map{ case (custID, (prodID, numberProd)) =>
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
      println(s"Sum number of products: $sumNumberProducts")*/

    println("Lab 6 finished")
  }
}
