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

/*    val lab1 = new Lab1(spark)
    lab1.lab1ReduceByKey()*/

    val lab7 = Lab7(spark)
    lab7.lab7SQLTables()

    val lab8 = Lab8(spark)
    lab8.Lab8TableColumns()

    val lab9 = Lab9(spark)
    lab9.Lab9ProcessingTables()

    Lab10(spark).lab10ProcessingTables()

    Lab11(spark).lab11WindowFunction()

  }

}
