package testChernov.labs

import testChernov.system._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

case class Lab8(spark: SparkSession) {
  def lab8TableColumns(): Unit = {
    println("Lab 8 started")

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
        "dateFormat" -> "dd.MM.yyyy"))
      .schema(Parameters.productSchema)
      .csv(Parameters.path_product)
      .withColumn("New_Price",
        when(col("Price") > 50000, col("Price")*0.9)
      .otherwise(col("Price")))
      .withColumn("Type", deviceUDF(col("Name")))
      .select("Name", "New_Price", "Type")
      .show()

    println("Lab 8 finished")
  }
}
