package testChernov.labs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

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

    val growMenDF = spark.read
      .options(Map(("delimiter", ";"),
        ("header", "true"),
        ("encoding", "cp1251"),
        ("dateFormat", "dd.MM.yyyy")))
      .schema(growSchema)
      .csv("dataset/population/Число мужчин на 1 января, Человек_Пермьстат_10 февраля 2014.csv")
      .withColumn("Value", regexp_replace('ValueStr, ",", ".").cast(DoubleType))
      .drop('ValueStr)

    val growWomenDF = spark.read
      .options(Map(("delimiter", ";"),
        ("header", "true"),
        ("encoding", "cp1251"),
        ("dateFormat", "dd.MM.yyyy")))
      .schema(growSchema)
      .csv("dataset/population/Число женщин на 1 января, Человек_Пермьстат_10 февраля 2014.csv")
      .withColumn("Value", regexp_replace('ValueStr, ",", ".").cast(DoubleType))
      .drop('ValueStr)

    val window = Window.orderBy('Year)

    println("Прирост мужского/женского населения по годам: ")
    val growMenDFAgg = growMenDF.withColumn("Year",
      date_format('Date, "yyyy").alias("Population growth"))
      .groupBy('Year)
      .agg(sum('Value).alias("Population"))
      .orderBy('Year)
      .withColumn("Lag", lag('Population, 1).over(window))
      .filter('Lag.isNotNull)
      .withColumn("grow men", 'Population - 'Lag)
      .drop("Population", "Lag")

    val growWomenDFAgg = growWomenDF.withColumn("Year",
      date_format('Date, "yyyy").alias("Population growth"))
      .groupBy('Year)
      .agg(sum('Value).alias("Population"))
      .orderBy('Year)
      .withColumn("Lag", lag('Population, 1).over(window))
      .filter('Lag.isNotNull)
      .withColumn("grow women", 'Population - 'Lag)
      .drop("Population", "Lag")

    growMenDFAgg.join(growWomenDFAgg, "Year").show()

    println("Прирост населения в г. Пермь: ")
    growDF.filter('Region === "г. Пермь")
      .withColumn("Year",
        date_format('Date, "yyyy").alias("Population growth"))
      .groupBy('Year)
      .agg(sum('Value))
      .orderBy('Year)
      .show(50, truncate = true)

    println("Прирост населения в области: ")
    growDF.filter('Region =!= "г. Пермь")
      .withColumn("Year",
        date_format('Date, "yyyy").alias("Population growth"))
      .groupBy('Year)
      .agg(sum('Value))
      .orderBy('Year)
      .show(50, truncate = true)

  }
}
