package testChernov.labs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

case class Lab8(spark: SparkSession) {
  def Lab8TableColumns(): Unit = {
    val productSchema = StructType(
      StructField("ID", IntegerType, nullable = false) ::
        StructField("Name", StringType, nullable = true) ::
        StructField("Price", DoubleType, nullable = true) ::
        StructField("Number_Of_Products", IntegerType, nullable = true) ::
        Nil)

    val getTypeDevice = (device: String) => {
      if (device.toLowerCase.contains("iphone")) {
        "phone"
      } else if (device.toLowerCase.contains("ipad")) {
        "tablet pc"
      } else if (device.toLowerCase.contains("macbook")) {
        "laptop"
      } else if (device.toLowerCase.contains("pods")) {
        "headphones"
      } else "other device"
    }

    val deviceUDF = udf(getTypeDevice)
    val productDF: Unit = spark.read
      .options(Map("delimiter" -> "\\t",
        "header" -> "true",
        "dateFormat" -> "dd.MM.yyyy"))
      .schema(productSchema)
      .csv("./dataset/product/product.csv")
      .withColumn("New_Price",
        when(col("Price") > 50000, col("Price")*0.9)
      .otherwise(col("Price")))
      .withColumn("Type", deviceUDF(col("Name")))
      .select("Name", "New_Price", "Type")
      .show()

  }
}
