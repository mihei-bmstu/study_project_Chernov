package testChernov.labs

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

case class Lab11(spark: SparkSession) {
  def lab11WindowFunction(): Unit = {
    println("Lab 11 started")
    import spark.implicits._

    val customerSchema = StructType(
      StructField("ID", IntegerType, nullable = false) ::
        StructField("Name", StringType, nullable = true) ::
        StructField("Email", StringType, nullable = true) ::
        StructField("Date", DateType, nullable = true) ::
        StructField("Status", StringType, nullable = true) ::
        Nil)
    val productSchema = StructType(
      StructField("ID", IntegerType, nullable = false) ::
        StructField("Name", StringType, nullable = true) ::
        StructField("Price", DoubleType, nullable = true) ::
        StructField("Number_Of_Products", IntegerType, nullable = true) ::
        Nil)
    val orderSchema = StructType(
      StructField("Customer_ID", IntegerType, nullable = false) ::
        StructField("Order_ID", IntegerType, nullable = false) ::
        StructField("Product_ID", IntegerType, nullable = false) ::
        StructField("Number_Of_Products", IntegerType, nullable = true) ::
        StructField("Order_Date", DateType, nullable = true) ::
        StructField("Status", StringType, nullable = true) ::
        Nil)

    val orderDF: DataFrame = spark.read
      .options(Map("delimiter" -> "\\t",
        "dateFormat" -> "dd.MM.yyyy"))
      .schema(orderSchema)
      .csv("./dataset/order/order.csv")

    val customerDF: DataFrame = spark.read
      .options(Map("delimiter" -> "\\t",
        "dateFormat" -> "dd.MM.yyyy"))
      .schema(customerSchema)
      .csv("./dataset/customer/customer.csv")
      .select('ID.as("cust_ID"),
        'email,
        'Name.as("customer_name"))

    val productDF: DataFrame = spark.read
      .options(Map("delimiter" -> "\\t",
        "dateFormat" -> "dd.MM.yyyy"))
      .schema(productSchema)
      .csv("./dataset/product/product.csv")
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
