package nl.vantriet

package object flight {

  case class AppConfig(
                       spark: SparkConfig,
                       topN: Int,
                       windowDuration: String,
                       slideDuration: String,
                       delayThreshold: String,
  )

  case class SparkConfig(
                    appName: String,
                    master: String,
                    source: String,
                    outputPath: String,
                    checkpointLocation: String
                  )

}
