package nl.vantriet.flight

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

class Flight(conf: AppConfig)(implicit spark: SparkSession) {

  import Flight._
  import spark.implicits._

  def read(): DataFrame = {
    val schema = new StructType()
      .add("airline", "string")
      .add("airlineId", "string")
      .add("srcAirport", "string")
      .add("srcAirportId", "string")
      .add("dstAirport", "string")
      .add("dstAirportId", "string")
      .add("codeshare", "string")
      .add("stops", "integer")
      .add("equipment", "string")

    spark
      .readStream
      .option("sep", ",")
      .schema(schema)
      .csv(conf.spark.source)
  }

  def topN(df: DataFrame, n: Int): DataFrame = {
    val timeColumn = window($"timestamp", conf.windowDuration, conf.slideDuration)
    val toString = udf{(window:GenericRowWithSchema) => window.mkString("-").replaceAll(":", "")}
    val topNUDF = udf{(array: Array[String]) => Flight.topN(array, n).mkString(",")}
    df
      .select($"srcAirport")
      .withColumn("timestamp", current_timestamp())
      .withWatermark("timestamp", conf.delayThreshold)
      .groupBy(timeColumn)
      .agg(collect_list($"srcAirport").as("airports"))
      .select(toString($"window").as("window"), topNUDF($"airports").as("top"))
  }

  def write(df: DataFrame): Unit = {
    df.writeStream
      .outputMode("append")
      .partitionBy("window")
      .format("csv")
      .option("sep", fieldSeparatorCharacter)
      .start(conf.spark.outputPath)
      .awaitTermination()
  }
}

object Flight {

  val fieldSeparatorCharacter = "\u001F"

  def apply(conf: AppConfig): Flight = {
    implicit val spark: SparkSession = SparkSession
      .builder
      .appName(conf.spark.appName)
      .master(conf.spark.master)
      .config("spark.sql.streaming.checkpointLocation", conf.spark.checkpointLocation)
      .getOrCreate()
    new Flight(conf)
  }

  def topN(array: Array[String], n:  Int): Seq[(String, Int)] = {
    array.foldLeft(Map.empty[String, Int]){
      (count, word) => count + (word -> (count.getOrElse(word, 0) + 1))
    }.toSeq
      .sortBy(- _._2)
      .take(n)
  }
}
