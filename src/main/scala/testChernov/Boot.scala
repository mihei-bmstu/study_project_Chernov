package testChernov

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import testChernov.labs._

object Boot {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("com").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("testChernov")
      .getOrCreate()

    import spark.implicits._

    /*val lab1 = new Lab1(spark)
    lab1.lab1ReduceByKey()*/

    Lab7(spark).lab7SQLTables()
    Lab8(spark).lab8TableColumns()
    Lab9(spark).lab9ProcessingTables()
    Lab10(spark).lab10ProcessingTables()
    Lab11(spark).lab11WindowFunction()
    Lab12(spark).lab12SQLCollections()

  }

}
