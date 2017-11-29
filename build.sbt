name := "bigdata-scala"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" %"1.6.0"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.0"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.6.0"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.0"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.6.0"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.17"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"