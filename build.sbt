name := "tookitaki"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(

"org.apache.spark" %% "spark-core" % "2.2.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.2.0" % "provided",
  "com.github.scopt" %% "scopt" % "3.7.0",
  "com.typesafe" % "config" % "1.3.1")
