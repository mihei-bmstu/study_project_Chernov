package testChernov

import org.apache.spark.sql.SparkSession
import testChernov.labs._

object Boot {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("testChernov")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val lab1 = new Lab1(spark)
    lab1.lab1ReduceByKey()

  }

}
