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

}