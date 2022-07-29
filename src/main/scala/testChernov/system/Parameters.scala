package testChernov.system

import org.apache.spark.sql.types._
import java.io.File

case class Order(Customer_ID: Integer,
                 Order_ID: Integer,
                 Product_ID: Integer,
                 Number_Of_Products: Integer,
                 Order_Date: String,
                 Status: String)

case class Product(ID: Integer,
                   Name: String,
                   Price: Double,
                   Number_Of_Products: Integer)

case class Customer(ID: Integer,
                    Name: String,
                    Email: String,
                    Date: String,
                    Status: String)

object Parameters {
  def getListOfFiles(dir: String): List[String] = {
    val file = new File(dir)
    file.listFiles.map(_.getName).toList
  }

  val path_customer = "./dataset/customer/customer.csv"
  val path_order = "./dataset/order/order.csv"
  val path_product = "./dataset/product/product.csv"
  val path_order_info = "./dataset/order-info/order-info.csv"

  val customerSchema: StructType = StructType(
    StructField("ID", IntegerType, nullable = false) ::
      StructField("Name", StringType, nullable = true) ::
      StructField("Email", StringType, nullable = true) ::
      StructField("Date", DateType, nullable = true) ::
      StructField("Status", StringType, nullable = true) ::
      Nil)
  val orderSchema: StructType = StructType(
    StructField("Customer_ID", IntegerType, nullable = false) ::
      StructField("Order_ID", IntegerType, nullable = false) ::
      StructField("Product_ID", IntegerType, nullable = false) ::
      StructField("Number_Of_Products", IntegerType, nullable = true) ::
      StructField("Order_Date", DateType, nullable = true) ::
      StructField("Status", StringType, nullable = true) ::
      Nil)
  val orderInfoSchema: StructType = StructType(
    StructField("ID", IntegerType, nullable = false) ::
      StructField("Departure_Date", DateType, nullable = true) ::
      StructField("Transfer_Date", DateType, nullable = true) ::
      StructField("Delivery_Date", DateType, nullable = true) ::
      StructField("Departure_City", StringType, nullable = true) ::
      StructField("Delivery_City", StringType, nullable = true) ::
      Nil)
  val productSchema: StructType = StructType(
    StructField("ID", IntegerType, nullable = false) ::
      StructField("Name", StringType, nullable = true) ::
      StructField("Price", DoubleType, nullable = true) ::
      StructField("Number_Of_Products", IntegerType, nullable = true) ::
      Nil)

}
