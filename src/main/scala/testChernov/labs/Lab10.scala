package testChernov.labs

import testChernov.system._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Lab10(spark:SparkSession) {
  def lab10ProcessingTables(): Unit = {
    println("Lab 10 started")
    import spark.implicits._

    val orderDF: DataFrame = spark.read
      .options(Map("delimiter" -> "\\t",
        "dateFormat" -> "dd.MM.yyyy"))
      .schema(Parameters.orderSchema)
      .csv(Parameters.path_order)
      .filter(
        col("Order_Date") > "2018-01-01" &&
        col("Order_Date") < "2018-07-01" &&
        col("Status") === "delivered")

    val customerDF: DataFrame = spark.read
      .options(Map("delimiter" -> "\\t",
        "dateFormat" -> "dd.MM.yyyy"))
      .schema(Parameters.customerSchema)
      .csv(Parameters.path_customer)
      .select('ID.as("cust_ID"),
        'email,
        'Name.as("customer_name"))

    val productDF: DataFrame = spark.read
      .options(Map("delimiter" -> "\\t",
        "dateFormat" -> "dd.MM.yyyy"))
      .schema(Parameters.productSchema)
      .csv(Parameters.path_product)
      .select('ID.as("prod_ID"),
        'name.as("product_name"),
        'Price)

    val customerJoinProduct = customerDF.crossJoin(productDF)

    orderDF.join(customerJoinProduct,
      orderDF("product_ID") === customerJoinProduct("prod_ID") &&
      orderDF("customer_ID") === customerJoinProduct("cust_ID"))
      .groupBy('customer_ID, 'product_ID)
      .agg(sum('Number_of_Products * 'Price).as("Total orders sum"),
        max('number_of_products).as("Max number of products"),
        min('Number_of_Products * 'Price).as("Min order "),
        avg('Number_of_Products * 'Price).as("Average order sum"))
      .show()

    println("Lab 10 finished")
  }
}
