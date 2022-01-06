package nl.vantriet.flight

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

class FlightSpec extends WordSpec with Matchers with BeforeAndAfter {

  val conf = AppConfig(spark=null, topN=3, windowDuration="5 seconds", slideDuration="1 second", delayThreshold="0 second")
  var spark: SparkSession = _

  val windowOverlap = 5

  before {
    spark = SparkSession
      .builder()
      .appName("FlightSpec")
      .master("local[1]")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .getOrCreate()
  }

  after {
    spark.stop()
  }

  "flight" should {

    "calculate top3" in {
      val airports = Array("A", "B", "C", "A", "B", "A")
      val expected = List(("A", 3), ("B", 2), ("C", 1)).toSeq
      val result = Flight.topN(airports, 3)
      result shouldEqual expected
    }

    "calculate top2" in {
      val airports = Array("A", "B", "C", "A", "B", "A")
      val expected = List(("A", 3), ("B", 2)).toSeq
      val result = Flight.topN(airports, 2)
      result shouldEqual expected
    }

    "calculate top N on DataFrame" in {
      implicit val spark_ = spark
      implicit val ssc = spark.sqlContext
      import ssc.implicits._

      val df = Seq(
        ("2B","410","AER","2965"),
        ("2B","410","ASF","2966"),
        ("2B","410","ASF","2966"),
      ).toDF("airline", "airlineId", "srcAirport", "srcAirportId")

      val flight = new Flight(conf)
      val result = flight.topN(df, 1)

      result.collectAsList().size() shouldEqual windowOverlap
      result.collectAsList().get(0).getString(1) shouldEqual "(ASF,2)"
    }
  }
}
