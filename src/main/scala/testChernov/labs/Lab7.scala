package testChernov.labs

import testChernov.system._
import org.apache.spark.sql.SparkSession

case class Lab7(spark: SparkSession) {
  def lab7SQLTables(n: Int = 10, trim: Boolean = true): Unit = {
    println("Lab 7 started")

    val customerDF: Unit = spark.read
      .options(Map("delimiter" -> "\\t",
        "dateFormat" -> "dd.MM.yyyy"))
      .schema(Parameters.customerSchema)
      .csv(Parameters.path_customer)
      .createOrReplaceTempView("customer")

    val orderDF: Unit = spark.read
      .options(Map("delimiter" -> "\\t",
        "dateFormat" -> "dd.MM.yyyy"))
      .schema(Parameters.orderSchema)
      .csv(Parameters.path_order)
      .createOrReplaceTempView("order")

    val productDF: Unit = spark.read
      .options(Map("delimiter" -> "\\t",
        "dateFormat" -> "dd.MM.yyyy"))
      .schema(Parameters.productSchema)
      .csv(Parameters.path_product)
      .createOrReplaceTempView("product")

    val response: Unit = spark.sql(
      """
        |SELECT DISTINCT customer_id, order_date FROM order WHERE status = 'delivered'
        |""".stripMargin).show(n, trim)

    println("Lab 7 finished")
  }
}
