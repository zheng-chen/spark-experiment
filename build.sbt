import AssemblyKeys._ // put this at the top of the file

assemblySettings

name := "spark"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += 	"org.apache.spark" %% "spark-core" % "1.0.0"  % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.0.0" % "provided"

libraryDependencies += 	"org.apache.hadoop" % "hadoop-client" % "2.2.0" % "provided"

libraryDependencies += 	"com.typesafe" % "config" % "1.0.2"

libraryDependencies +=	"com.typesafe.play" %% "play-json" % "2.2.1"

libraryDependencies ++= Seq(
  "org.mongodb" %% "casbah" % "2.7.3",
  "org.scalatest" %% "scalatest" % "1.9.2",
  "junit" % "junit" % "4.11" % "test"
)

retrieveManaged := false

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"

jarName in assembly := "SSI-Spark.jar"

test in assembly := {}
