scalaVersion := "2.13.11"
name := "kawka-scream"

val KafkaVersion = "3.4.0"
libraryDependencies ++= Seq(
    "org.apache.kafka" % "kafka-clients" % KafkaVersion,
    "org.apache.kafka" % "kafka-streams" % KafkaVersion,
    "org.apache.kafka" %% "kafka-streams-scala" % KafkaVersion,
    "io.circe" %% "circe-core" % "0.14.5",
    "io.circe" %% "circe-generic" % "0.14.5",
    "io.circe" %% "circe-parser" % "0.14.5"
)

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5"
libraryDependencies += "org.slf4j" % "slf4j-simple" % "2.0.5"

val AllTest = "test,it"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.15" % AllTest
libraryDependencies += "org.apache.kafka" % "kafka-streams-test-utils" % KafkaVersion % AllTest

//for org.apache.kafka.test.TestUtils
//see https://stackoverflow.com/a/8076568/1964213
libraryDependencies += "org.apache.kafka" % "kafka-clients" % KafkaVersion % AllTest classifier "test"
