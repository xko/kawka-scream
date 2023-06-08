scalaVersion := "2.13.11"
name := "kawka-scream"

libraryDependencies ++= Seq(
    "org.apache.kafka" % "kafka-clients" % "3.4.0",
    "org.apache.kafka" % "kafka-streams" % "3.4.0",
    "org.apache.kafka" %% "kafka-streams-scala" % "3.4.0",
    "io.circe" %% "circe-core" % "0.14.5",
    "io.circe" %% "circe-generic" % "0.14.5",
    "io.circe" %% "circe-parser" % "0.14.5"
)

libraryDependencies += "org.slf4j" % "slf4j-simple" % "2.0.5"