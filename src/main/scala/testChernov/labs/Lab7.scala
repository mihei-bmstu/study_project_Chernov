package testChernov.labs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

case class Lab7(spark: SparkSession) {
  def lab7SQLTables(n: Int = 10, trim: Boolean = true): Unit = {
    println("Lab 7 started")

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

    val customerDF: Unit = spark.read
      .options(Map("delimiter" -> "\\t",
        "dateFormat" -> "dd.MM.yyyy"))
      .schema(customerSchema)
      .csv("./dataset/customer/customer.csv")
      .createOrReplaceTempView("customer")

    val orderDF: Unit = spark.read
      .options(Map("delimiter" -> "\\t",
        "dateFormat" -> "dd.MM.yyyy"))
      .schema(orderSchema)
      .csv("./dataset/order/order.csv")
      .createOrReplaceTempView("order")

    val productDF: Unit = spark.read
      .options(Map("delimiter" -> "\\t",
        "dateFormat" -> "dd.MM.yyyy"))
      .schema(productSchema)
      .csv("./dataset/product/product.csv")
      .createOrReplaceTempView("product")

    val response: Unit = spark.sql(
      """
        |SELECT DISTINCT customer_id, order_date FROM order WHERE status = 'delivered'
        |""".stripMargin).show(n, trim)

    println("Lab 7 finished")
  }
}
