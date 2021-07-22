name := "mimir-api"
version := "1.1.1-SNAPSHOT"
organization := "org.mimirdb"
scalaVersion := "2.12.10"

// Make the UX work in SBT
fork := true
outputStrategy in run := Some(StdoutOutput)
connectInput in run := true
cancelable in Global := true

// Produce Machine-Readable JUnit XML files for tests
testOptions in Test ++= Seq( Tests.Argument("junitxml"), Tests.Argument("console") )

// Specs2 Requirement:
scalacOptions in Test ++= Seq("-Yrangepos")

// Support Test Resolvers
resolvers += "MimirDB" at "https://maven.mimirdb.info/"
resolvers += Resolver.typesafeRepo("releases")
resolvers += DefaultMavenRepository
resolvers ++= Seq("snapshots", "releases").map(Resolver.sonatypeRepo)
resolvers += Resolver.mavenLocal
resolvers += "Open Source Geospatial Foundation Repository" at "https://repo.osgeo.org/repository/release/"
// resolvers += "Java.net repository" at "https://download.java.net/maven/2"

excludeDependencies ++= Seq(
  // Hadoop brings in more logging backends.  Kill it with fire.
  ExclusionRule("org.slf4j", "slf4j-log4j12"),
  // Chained jetty dependencies
  ExclusionRule( organization = "org.mortbay.jetty"), 
)

// Custom Dependencies
libraryDependencies ++= Seq(

  // Jetty
  "org.eclipse.jetty"             %   "jetty-http"               % "9.4.10.v20180503",
  "org.eclipse.jetty"             %   "jetty-io"                 % "9.4.10.v20180503",
  "org.eclipse.jetty"             %   "jetty-security"           % "9.4.10.v20180503",
  "org.eclipse.jetty"             %   "jetty-server"             % "9.4.10.v20180503",
  "org.eclipse.jetty"             %   "jetty-servlet"            % "9.4.10.v20180503",
  "org.eclipse.jetty"             %   "jetty-servlets"           % "9.4.10.v20180503",
  "org.eclipse.jetty"             %   "jetty-util"               % "9.4.10.v20180503",
  "org.eclipse.jetty"             %   "jetty-webapp"             % "9.4.10.v20180503",
  "javax.servlet"                 %   "javax.servlet-api"        % "3.1.0",
  
  // Mimir
  "org.mimirdb"                   %% "mimir-caveats"             % "0.3.4" excludeAll( 
                                                                             ExclusionRule("com.fasterxml.jackson.core"), 
                                                                             ExclusionRule(organization ="org.apache.httpcomponents", name="httpcore"),
                                                                             ExclusionRule(organization = "javax.servlet")
                                                                           ),
  // "org.mimirdb"                   %% "mimir-vizual"              % "0.1-SNAPSHOT",

  // API
  "com.typesafe.scala-logging"    %%  "scala-logging"            % "3.9.2",
  "ch.qos.logback"                %   "logback-classic"          % "1.2.3",
  "org.apache.logging.log4j"      %   "log4j-core"               % "2.13.3",
  "org.rogach"                    %%  "scallop"                  % "3.4.0",

  // Testing
  "org.specs2"                    %%  "specs2-core"              % "4.8.2" % "test",
  "org.specs2"                    %%  "specs2-matcher-extra"     % "4.8.2" % "test",
  "org.specs2"                    %%  "specs2-junit"             % "4.8.2" % "test",

  // Play JSON
  "com.typesafe.play"             %%  "play-json"                % "2.8.1",

  // Metadata Backends
  "org.xerial"                    %   "sqlite-jdbc"              % "3.16.1",

  //Data Source Support
  //"com.amazonaws"               %   "aws-java-sdk-bundle"      % "1.11.375",
  //"org.apache.hadoop"           %   "hadoop-aws"               % "3.2.0",
  //"com.amazonaws"                 %   "aws-java-sdk-core"        % "1.10.6"   excludeAll( ExclusionRule("com.fasterxml.jackson.core")),
  //"com.amazonaws"                 %   "aws-java-sdk-s3"          % "1.10.6"   excludeAll( ExclusionRule("com.fasterxml.jackson.core")),
  "org.apache.hadoop"             %   "hadoop-aws"               % "2.8.3"    excludeAll( 
                                                                                  ExclusionRule("com.fasterxml.jackson.core"), ExclusionRule(organization ="com.amazonaws"), 
                                                                                  ExclusionRule(organization ="org.apache.httpcomponents", name="httpcore"),
                                                                                  ExclusionRule(organization = "javax.servlet")
                                                                                ),
  "com.amazonaws"                 %   "aws-java-sdk-core"        % "1.11.199" excludeAll( ExclusionRule("com.fasterxml.jackson.core")),
  "com.amazonaws"                 %   "aws-java-sdk-s3"          % "1.11.199" excludeAll( ExclusionRule("com.fasterxml.jackson.core")),

  //Scala eval support
  "org.scala-lang"                %   "scala-reflect"            % scalaVersion.value,
  "org.scala-lang"                %   "scala-compiler"           % scalaVersion.value,

  //GIS Support
  //Local
  "org.apache.sedona"             %% "sedona-core-3.0"           % "1.0.0-incubating" excludeAll(ExclusionRule(organization ="javax.servlet"), ExclusionRule("com.fasterxml.jackson.core")),
  "org.apache.sedona"             %% "sedona-sql-3.0"            % "1.0.0-incubating" excludeAll(ExclusionRule(organization ="javax.servlet"), ExclusionRule("com.fasterxml.jackson.core"), ExclusionRule(organization ="org.datasyslab", name="sernetcdf")),
  "org.apache.sedona"             %% "sedona-viz-3.0"            % "1.0.0-incubating" excludeAll(ExclusionRule(organization ="javax.servlet"), ExclusionRule("com.fasterxml.jackson.core"), ExclusionRule("org.apache.hadoop"), ExclusionRule("org.apache.http"), ExclusionRule(organization ="org.datasyslab", name="sernetcdf")),
  "org.locationtech.jts"          %  "jts-core"                  % "1.18.0",
  "org.wololo"                    %  "jts2geojson"               % "0.14.3",
  "org.geotools"                  %  "gt-main"                   % "24.0",
  "org.geotools"                  %  "gt-referencing"            % "24.0",
  "org.geotools"                  %  "gt-epsg-hsql"              % "24.0",
  //Snapshot Of MB's PR
  //"org.datasyslab"                %  "geospark"                  % "1.3.2-SNAPSHOT" excludeAll(ExclusionRule(organization ="javax.servlet")),
  //"org.datasyslab"                %  "geospark-sql_3.0"          % "1.3.2-SNAPSHOT" excludeAll(ExclusionRule(organization ="javax.servlet"), ExclusionRule(organization ="org.datasyslab", name="sernetcdf")),
  //"org.datasyslab"                %  "geospark-viz_3.0"          % "1.3.2-SNAPSHOT" excludeAll(ExclusionRule(organization ="javax.servlet"), ExclusionRule("org.apache.http"), ExclusionRule(organization ="org.datasyslab", name="sernetcdf")),
  
  //Other data importers
  "com.databricks"                %% "spark-xml"                 % "0.9.0",

  //Google Sheets Datasource
  "info.mimirdb"                  %% "spark-google-spreadsheets" % "0.6.4",

  //excel data loading
  "com.crealytics"                %%  "spark-excel"              % "0.13.3+17-b51cc0ac+20200722-1201-SNAPSHOT" excludeAll(
                                                                      ExclusionRule(organization = "javax.servlet")
                                                                    ),


)

////// Generate a Coursier Bootstrap Jar
// See https://get-coursier.io/docs
//
import scala.sys.process.{Process, ProcessLogger}
lazy val bootstrap = taskKey[Unit]("Generate Bootstrap Jar")
bootstrap := {
  val logger = ProcessLogger(println(_), println(_))
  val coursier_bin = "bin/coursier"
  val coursier_url = "https://git.io/coursier-cli"
  val mimir_bin = "bin/mimir-api"
  if(!java.nio.file.Files.exists(java.nio.file.Paths.get("bin/coursier"))){

    println("Downloading Coursier...")
    Process(List(
      "curl", "-L",
      "-o", coursier_bin,
      coursier_url
    )) ! logger match {
      case 0 =>
      case n => sys.error(s"Could not download Coursier")
    }
    Process(List(
      "chmod", "+x", coursier_bin
    )) ! logger
    println("... done")
  }

  println("Coursier available.  Generating Repository List")

  val resolverArgs = resolvers.value.map {
    case r: MavenRepository => Seq("-r", r.root)
  }.flatten

  val (art, file) = packagedArtifact.in(Compile, packageBin).value
  val qualified_artifact_name = file.name.replace(".jar", "").replaceFirst("-SNAPSHOT","").replaceFirst("-([0-9.]+)$", "")
  val full_artifact_name = s"${organization.value}:${qualified_artifact_name}:${version.value}"
  println("Rendering bootstraps for "+full_artifact_name)
  for(resolver <- resolverArgs){
    println("  "+resolver)
  }

  println("Generating Mimir-API Server binary")
  Process(List(
    coursier_bin,
    "bootstrap",
    full_artifact_name,
    "-f",
    "-o", "bin/mimir-api",
    "-r", "central",
    "-M", "org.mimirdb.api.MimirAPI",
    "--embed-files=false"
  )++resolverArgs) ! logger match {
      case 0 =>
      case n => sys.error(s"Bootstrap failed")
  }
}


////// Publishing Metadata //////
// use `sbt publish make-pom` to generate
// a publishable jar artifact and its POM metadata

publishMavenStyle := true

pomExtra := <url>http://mimirdb.info</url>
  <licenses>
    <license>
      <name>Apache License 2.0</name>
      <url>http://www.apache.org/licenses/</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>git@github.com:ubodin/mimir-api.git</url>
    <connection>scm:git:git@github.com:ubodin/mimir-api.git</connection>
  </scm>

/////// Publishing Options ////////
// use `sbt publish` to update the package in
// your own local ivy cache
publishTo := Some(Resolver.file("file",  new File("/var/www/maven_repo/")))
// publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))
