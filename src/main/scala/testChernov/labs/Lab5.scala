package testChernov.labs

import org.apache.spark.sql.SparkSession
import testChernov.system._

case class Lab5(spark: SparkSession) {
  def lab5GroupBy(): Unit = {
    println("Lab 5 started")
    import spark.implicits._

    val orderRDD = spark.read
      .option("delimiter", "\t")
      .schema(Parameters.orderSchema)
      .csv(Parameters.path_order)
      .as[Order]
      .rdd
      .filter(_.Status == "delivered")
      .map{ case Order(custID, _, _, numberProd, _, _) =>
        (custID, (numberProd, 1))}
      .reduceByKey { case ((v1, c1), (v2, c2)) => (v1 + v2, c1 + c2)}
      .mapValues { case (v, c) =>  (v, c, v.toDouble / c.toDouble)}

    println("Customer ID, (Sum number of products, number of orders, average order)")
    orderRDD.foreach(println)
    println("Lab 5 finished")
  }
}
