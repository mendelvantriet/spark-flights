package nl.vantriet.flight

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._

object App extends LazyLogging {

  def main(args: Array[String]): Unit = {
    logger.info("Starting...")
    val config: Config = ConfigFactory.load
    val appConfig = config.as[AppConfig]

    val flight = Flight(appConfig)
    val df = flight.read()
    val topList = flight.topN(df, appConfig.topN)
    flight.write(topList)
  }
}
