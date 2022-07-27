package testChernov.labs

import testChernov.system._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window

case class Lab11(spark: SparkSession) {
  def lab11WindowFunction(): Unit = {
    println("Lab 11 started")
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
      .select('ID.as("cust_ID"),
        'email,
        'Name.as("customer_name"))

    val productDF: DataFrame = spark.read
      .options(Map("delimiter" -> "\\t",
        "dateFormat" -> "dd.MM.yyyy"))
      .schema(Parameters.productSchema)
      .csv(Parameters.path_product)
      .select('ID.as("prod_ID"),
        'name.as("product_name"))

    val customerJoinProduct = customerDF.crossJoin(productDF)

    orderDF.groupBy('customer_id, 'product_id)
      .agg(sum('number_of_products).as("sum_num_of_products"))
      .withColumn("rn", row_number.over(
        Window.partitionBy("customer_id")
          .orderBy('sum_num_of_products.desc)))
      .filter('rn === 1)
      .join(customerJoinProduct,
        'product_ID === customerJoinProduct("prod_ID") &&
        'customer_ID === customerJoinProduct("cust_ID"))
      .select('customer_name, 'product_name)
      .show()

    println("Lab 11 finished")
  }
}
