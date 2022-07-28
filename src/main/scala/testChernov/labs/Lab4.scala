package testChernov.labs

import org.apache.spark.sql.SparkSession
import testChernov.system._

case class Lab4(spark:SparkSession) {
  def lab4CompositeKey(): Unit = {
    println("Lab 4 started")
    import spark.implicits._

    val orderRDD = spark.read
      .option("delimiter", "\t")
      .schema(Parameters.orderSchema)
      .csv(Parameters.path_order)
      .as[Order]
      .rdd
      .map { case Order(custID, orderID, prodID, numberProd, orderDate, status) =>
        ((custID, prodID), numberProd)}

    val customerRDD = spark.read
      .option("delimiter", "\t")
      .schema(Parameters.customerSchema)
      .csv(Parameters.path_customer)
      .as[Customer]
      .rdd

    val productRDD = spark.read
      .option("delimiter", "\t")
      .schema(Parameters.productSchema)
      .csv(Parameters.path_product)
      .as[Product]
      .rdd

    val custJoinProd = customerRDD.cartesian(productRDD)
      .map{ case (Customer(custID, custName, email, date, status),
      Product(prodID, prodName, price, numberProd)) => ((custID, prodID), (custName, prodName, price)) }

    val joinedRDD = orderRDD.leftOuterJoin(custJoinProd)
      .map{ case ((_, _),(z1, z2: Option[(String, String, Double)]))
        => (z1, z2.getOrElse(("default", "default", 0.0))) }
      .map{ case (num, (cName, pName, price)) => (cName, pName, num*price)}

    joinedRDD.foreach(println)
    println("Lab 4 finished")
  }
}
