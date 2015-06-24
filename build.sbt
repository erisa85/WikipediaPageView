import AssemblyKeys._

name := "WikipediaPageView"

version := "1.0"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.3.1" % "provided"
)


assemblySettings

test in assembly := {}

jarName in assembly := "wikipedia-1_0.jar"

mainClass in assembly := Some("wiki.Wikipedia")