ThisBuild / scalaVersion := "2.12.20"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / name := "spark-datasources"

lazy val sparkVersion = "3.2.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  // Protobuf functions backport for Spark 3.2.1 (shaded)
  "com.example" %% "spark-protobuf-backport-shaded" % "0.1.0-SNAPSHOT",
  // Hadoop 4mc codec (published via spark-4mc/java/hadoop-4mc)
  "com.fing.fourmc" % "hadoop-4mc" % "3.0.0" % Provided
)

// do not generate scaladocs; we rely on compiled jar
//Compile / doc / scalacOptions ++= Seq("-no-link-warnings")
