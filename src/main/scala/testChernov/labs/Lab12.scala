package testChernov.labs

import testChernov.system._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

case class Lab12(spark: SparkSession) {
  def lab12SQLCollections(): Unit ={
    println("Lab 12 started")
    import spark.implicits._

    val customerDF: DataFrame = spark.read
      .options(Map("delimiter" -> "\\t",
        "dateFormat" -> "dd.MM.yyyy"))
      .schema(Parameters.customerSchema)
      .csv(Parameters.path_customer)
      .filter('Name === "John")
      .drop('Status)

    val orderDF: DataFrame = spark.read
      .options(Map("delimiter" -> "\\t",
        "dateFormat" -> "dd.MM.yyyy"))
      .schema(Parameters.orderSchema)
      .csv(Parameters.path_order)

    val ordersJohn = orderDF.join(customerDF, 'customer_ID === 'ID)
      .filter('Status === "delivered")
      .drop('ID)

    val orderInfoDF: DataFrame = spark.read
      .options(Map("delimiter" -> "\\t",
        "dateFormat" -> "dd.MM.yyyy"))
      .schema(Parameters.orderInfoSchema)
      .csv(Parameters.path_order_info)

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
