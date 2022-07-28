package testChernov.labs

import org.apache.spark.sql.SparkSession
import testChernov.system._

case class Lab3(spark: SparkSession) {
  def lab3Join(): Unit = {
    println("Lab 3 started")
    import spark.implicits._

    val orderRDD = spark.read
      .option("delimiter", "\t")
      .schema(Parameters.orderSchema)
      .csv(Parameters.path_order)
      .as[Order]
      .rdd
      .map { case Order(x1, _, x3, x4, x5, _) => (x1, (x3, x4, x5))}

    val customerRDD = spark.read
      .option("delimiter", "\t")
      .schema(Parameters.customerSchema)
      .csv(Parameters.path_customer)
      .as[Customer]
      .rdd
      .map { case Customer(x1, x2, _, _, _) => (x1, x2)}

    val joinedRDD = orderRDD.join(customerRDD)
      .map {case (custID, ((prodID, numberProd, orderDate), cust_name)) =>
        (prodID, (cust_name, orderDate, numberProd))}

    val productRDD = spark.read
      .option("delimiter", "\t")
      .schema(Parameters.productSchema)
      .csv(Parameters.path_product)
      .as[Product]
      .rdd
      .map { case Product(id, name, price, numberProd) => (id, price)}

    val joinedProduct = joinedRDD.join(productRDD)
      .map { case (id ,((custName, orderDate, prodNumber), price)) =>
        ((custName, orderDate), prodNumber * price)}
      .reduceByKey(_ + _)

    println("(Customer, Date), Sum orders")
    joinedProduct.foreach(println)
    print("Lab 3 finished")
  }
}
