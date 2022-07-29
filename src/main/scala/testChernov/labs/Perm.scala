package testChernov.labs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

case class Perm(spark: SparkSession) {
  def permStat(): Unit = {
    import spark.implicits._

    val growSchema = StructType(Seq(
      StructField("Level", IntegerType, nullable = true),
      StructField("Date", DateType, nullable = true),
      StructField("Type", StringType, nullable = true),
      StructField("Region", StringType, nullable = true),
      StructField("ValueStr", StringType, nullable = true)
    ))

    val growDF = spark.read
      .options(Map(("delimiter", ";"),
        ("header", "true"),
        ("encoding", "cp1251"),
        ("dateFormat", "dd.MM.yyyy")))
      .schema(growSchema)
      .csv("dataset/population/Естественный прирост населения, Человек_Пермьстат_10 мая 2014.csv")
      .withColumn("Value", regexp_replace('ValueStr, ",", ".").cast(DoubleType))
      .drop('ValueStr)

    println("Прирост населения по годам: ")
    growDF.withColumn("Year",
      date_format('Date, "yyyy").alias("Population growth"))
      .groupBy('Year)
      .agg(sum('Value))
      .orderBy('Year)
      .show(50, truncate = true)


  }
}
