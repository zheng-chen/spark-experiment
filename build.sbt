import AssemblyKeys._ // put this at the top of the file

assemblySettings

name := "spark"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.0.2",
  "com.typesafe.play" %% "play-json" % "2.2.1",  
  "org.mongodb" %% "casbah" % "2.7.3",
  "org.scalatest" %% "scalatest" % "1.9.2",
  "junit" % "junit" % "4.11" % "test",
  "ooyala.cnd" % "job-server" % "0.3.1" % "provided",
  "org.apache.spark" %% "spark-core" % "1.0.2"  % "provided",
  "org.apache.spark" %% "spark-sql" % "1.0.2" % "provided",
  "org.apache.spark" %% "spark-mllib" % "1.0.2" % "provided",
  "org.apache.hadoop" % "hadoop-client" % "2.2.0" % "provided"
)

retrieveManaged := false

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "Ooyala Bintray" at "http://dl.bintray.com/ooyala/maven"

jarName in assembly := "SSI-Spark.jar"

test in assembly := {}
