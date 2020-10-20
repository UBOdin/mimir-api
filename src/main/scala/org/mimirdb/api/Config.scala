package org.mimirdb.api

import org.rogach.scallop._

class MimirConfig(arguments: Seq[String]) extends ScallopConf(arguments)
{
  val metadata = opt[String]("driver", 
    descr = "Which metadata backend to use? ([sqlite])",
    default = Some("sqlite")
  )
  val staging = opt[String]("staging-dir", 
    descr = "A directory to stage temporary files in.",
    default = Some("vizier_downloads")
  )
  val port = opt[Int]("port", 
    descr = "The port to host the API on",
    default = Some(MimirAPI.DEFAULT_API_PORT)
  )
  val googleAPIKey = opt[String]("google-api-key", 
    descr = "Your Google API Key (for Geocoding)"
  )
  val osmServer = opt[String]("osm-server",
    descr = "Your Open Street Maps server (for Geocoding)"
  )
  val dataDir = opt[String]("data-dir",
    descr = "A directory to store metadata files in.",
    default = Some("vizier-data")
  )
  val sparkHost = opt[String]("spark-host",
    descr = "spark master.",
    default = Some("local")
  )
  val pythonPath = opt[String]("python", 
    descr = "Path to python binary",
    default = None
  )
  val experimental = opt[List[String]]("X", default = Some(List[String]()))
}