import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

object DemoDDApp extends Build {
  lazy val buildSettings = Defaults.defaultSettings ++ Seq(
    version := "1.0",
    scalaVersion := "2.10.4"
  )

  lazy val app = Project(
    "DemoDDApp",
    file("."),
    settings = buildSettings ++ assemblySettings ++ Seq(
      libraryDependencies ++= Seq(
        "com.datastax.spark" %% "spark-cassandra-connector" % "1.4.0-M1",
        "org.apache.spark" %% "spark-catalyst" % "1.4.0" % "provided"
      )
    )
  )
}
