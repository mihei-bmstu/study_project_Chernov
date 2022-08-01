package testChernov.labs

import org.apache.spark.sql.SparkSession
import testChernov.system._

case class Lab2(spark: SparkSession) {
  def lab2Join(): Unit = {
    println("Lab 2 started")
    import spark.implicits._

    val productRDD = spark.read
      .option("delimiter", "\t")
      .schema(Parameters.productSchema)
      .csv(Parameters.path_product)
      .as[Product]
      .rdd
      .map { case Product(x1, x2, _, _) => (x1, x2) }

    val orderRDD = spark.read
      .option("delimiter", "\t")
      .schema(Parameters.orderSchema)
      .csv(Parameters.path_order)
      .as[Order]
      .rdd
      .map { case Order(_, _, x3, x4, _, _) => (x3, x4) }

    val productSoldGrouped = orderRDD.reduceByKey(_ + _)
    println(s"Total products: " + productSoldGrouped.map(_._2).reduce(_ + _))

    val joinedRDD = productRDD.leftOuterJoin(productSoldGrouped)
   // val filteredJoinedRDD = joinedRDD.filter(x => x._2._2.isEmpty) //Никогда так не писать
   //val filteredJoinedRDD = joinedRDD.filter { case (_, (_, optionalField)) => optionalField.isEmpty }
   val filteredJoinedRDD = joinedRDD.flatMap { case (_, (value, opField)) =>
      opField.map(_ => value)
    }


    println("Products with no orders: ")
    filteredJoinedRDD.foreach(println)
    println("Lab 2 finished")
  }
}
