package testChernov.labs

import testChernov.system._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

case class Lab9(spark: SparkSession) {
  def lab9ProcessingTables(): Unit = {
    println("Lab 9 started")
    import spark.implicits._

    val orderDF: DataFrame = spark.read
      .options(Map("delimiter" -> "\\t",
        "dateFormat" -> "dd.MM.yyyy"))
      .schema(Parameters.orderSchema)
      .csv(Parameters.path_order)

    val customerDF: DataFrame = spark.read
      .options(Map("delimiter" -> "\\t",
        "dateFormat" -> "dd.MM.yyyy"))
      .schema(Parameters.customerSchema)
      .csv(Parameters.path_customer)
      .select(col("ID").as("customer_ID"), col("email"))

    val productDF: DataFrame = spark.read
      .options(Map("delimiter" -> "\\t",
        "dateFormat" -> "dd.MM.yyyy"))
      .schema(Parameters.productSchema)
      .csv(Parameters.path_product)
      .select(col("ID").as("product_ID"),
        col("name").as("product_name"))

    val joinedDF = customerDF.crossJoin(productDF)

    val orderDFFiltered = orderDF.filter(
      col("Order_Date") > "2018-01-01" &&
      col("Order_Date") < "2018-07-01" &&
      col("Status") === "delivered")

      orderDFFiltered.join(broadcast(productDF), orderDFFiltered("product_ID") === productDF("product_ID"))
        .join(broadcast(customerDF), orderDFFiltered("customer_ID") === customerDF("customer_ID"))
        .select('email, 'product_name, 'order_date, 'number_of_products)
        .orderBy('email, 'order_date)
      .show()
    println("Lab 9 finished")

  }
}
