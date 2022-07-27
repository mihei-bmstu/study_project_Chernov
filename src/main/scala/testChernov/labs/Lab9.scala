package testChernov.labs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

case class Lab9(spark: SparkSession) {
  def Lab9ProcessingTables(): Unit = {
    println("Lab 9 started")
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
      .select(col("ID").as("customer_ID"), col("email"))

    val productDF: DataFrame = spark.read
      .options(Map("delimiter" -> "\\t",
        "dateFormat" -> "dd.MM.yyyy"))
      .schema(productSchema)
      .csv("./dataset/product/product.csv")
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

  }
}
