// name                := "InfoFlow"
// version             := "1.1.1"
// scalaVersion        := "2.11.7" //"2.12.1"
// parallelExecution   := false
// libraryDependencies ++= Seq(
//         "org.apache.spark" %% "spark-core" % "2.1.1",
//         "org.scalatest" %% "scalatest" % "3.0.1" % "test",
//         "org.apache.spark" %% "spark-sql" % "2.1.1" % "provided"
// )

name := "InfoMap Test"

version := "1.1.1"

scalaVersion := "2.12.15"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-graphx" % "3.3.0",
"org.apache.spark" %% "spark-sql" % "3.3.0"
)
