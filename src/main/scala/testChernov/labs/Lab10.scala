package testChernov.labs

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Lab10(spark:SparkSession) {
  def lab10ProcessingTables(): Unit = {
    println("Lab 10 started")
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
      .filter(
        col("Order_Date") > "2018-01-01" &&
        col("Order_Date") < "2018-07-01" &&
        col("Status") === "delivered")

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
