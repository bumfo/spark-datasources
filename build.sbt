ThisBuild / scalaVersion := "2.12.20"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / name := "spark-datasources"

lazy val sparkVersion = "3.2.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  //  "com.github.fingltd" % "hadoop-4mc" % "3.0.0"
)

// do not generate scaladocs; we rely on compiled jar
//Compile / doc / scalacOptions ++= Seq("-no-link-warnings")
