package testChernov

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import testChernov.labs._
import testChernov.system.Parameters

object Boot {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("com").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("testChernov")
      .getOrCreate()

    Lab1(spark).lab1ReduceByKey()
    Lab2(spark).lab2Join()
    Lab3(spark).lab3Join()
    Lab4(spark).lab4CompositeKey()
    Lab5(spark).lab5GroupBy()
    Lab6(spark).lab6AggregateBy()
    Lab7(spark).lab7SQLTables()
    Lab8(spark).lab8TableColumns()
    Lab9(spark).lab9ProcessingTables()
    Lab10(spark).lab10ProcessingTables()
    Lab11(spark).lab11WindowFunction()
    Lab12(spark).lab12SQLCollections()

  }

}
