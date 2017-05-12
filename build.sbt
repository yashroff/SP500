import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.11.11",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "Hello",
    libraryDependencies ++= Seq( 
    "org.apache.spark" % "spark-core_2.11" % "2.1.1",
    "org.apache.spark" % "spark-mllib_2.11" % "2.1.1",
    "org.apache.spark" % "spark-sql_2.11" % "2.1.1",
    "nz.ac.waikato.cms.weka" % "weka-stable" % "3.6.6"
  ) 
)
