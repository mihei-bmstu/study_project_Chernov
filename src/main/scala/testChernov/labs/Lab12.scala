package testChernov.labs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

case class Lab12(spark: SparkSession) {
  def lab12SQLCollections(): Unit ={
    println("Lab 12 started")
    import spark.implicits._

    val customerSchema = StructType(
      StructField("ID", IntegerType, nullable = false) ::
        StructField("Name", StringType, nullable = true) ::
        StructField("Email", StringType, nullable = true) ::
        StructField("Date", DateType, nullable = true) ::
        StructField("Status", StringType, nullable = true) ::
        Nil)
    val orderSchema = StructType(
      StructField("Customer_ID", IntegerType, nullable = false) ::
        StructField("Order_ID", IntegerType, nullable = false) ::
        StructField("Product_ID", IntegerType, nullable = false) ::
        StructField("Number_Of_Products", IntegerType, nullable = true) ::
        StructField("Order_Date", DateType, nullable = true) ::
        StructField("Status", StringType, nullable = true) ::
        Nil)
    val orderInfoSchema = StructType(
      StructField("ID", IntegerType, nullable = false) ::
        StructField("Departure_Date", DateType, nullable = true) ::
        StructField("Transfer_Date", DateType, nullable = true) ::
        StructField("Delivery_Date", DateType, nullable = true) ::
        StructField("Departure_City", StringType, nullable = true) ::
        StructField("Delivery_City", StringType, nullable = true) ::
        Nil)

    val customerDF: DataFrame = spark.read
      .options(Map("delimiter" -> "\\t",
        "dateFormat" -> "dd.MM.yyyy"))
      .schema(customerSchema)
      .csv("./dataset/customer/customer.csv")
      .filter('Name === "John")
      .drop('Status)

    val orderDF: DataFrame = spark.read
      .options(Map("delimiter" -> "\\t",
        "dateFormat" -> "dd.MM.yyyy"))
      .schema(orderSchema)
      .csv("./dataset/order/order.csv")

    val ordersJohn = orderDF.join(customerDF, 'customer_ID === 'ID)
      .filter('Status === "delivered")
      .drop('ID)

    val orderInfoDF: DataFrame = spark.read
      .options(Map("delimiter" -> "\\t",
        "dateFormat" -> "dd.MM.yyyy"))
      .schema(orderInfoSchema)
      .csv("./dataset/order-info/order-info.csv")

    val orderInfoArray = orderInfoDF
      .select('ID,
        array('Departure_Date, 'Transfer_Date, lit("transfer")).as("transfer"),
        array('Transfer_date, 'Delivery_Date, lit("delivered")).as("delivered"))

    orderInfoArray.join(ordersJohn, 'ID === 'Order_ID)
      .select('Order_ID,
        explode('transfer),
        'Product_ID,
        'Number_Of_Products,
        'Name)
      .show(20, truncate = false)
    println("Lab 12 finished")

  }
}
