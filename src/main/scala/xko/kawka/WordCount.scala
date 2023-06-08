package xko.kawka

import java.time.Duration
import java.util.Properties
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import scala.io.StdIn.readLine
import scala.util.Using

object WordCount {

    import org.apache.kafka.streams.scala.serialization.Serdes._
    import org.apache.kafka.streams.scala.ImplicitConversions._

    def topo(fromTopic: String, toTopic: String) = {
        val builder = new StreamsBuilder()
        val textLines: KStream[String, String] = builder.stream[String, String](fromTopic)
        val wordCounts: KTable[String, Long] = textLines.flatMapValues(textLine => textLine.toLowerCase.split("\\W+"))
                                                        .groupBy((_, word) => word)
                                                        .count()
        wordCounts.toStream.to(toTopic)
        builder.build()
    }


    def main(args: Array[String]): Unit = {
        val config: Properties = {
            val p = new Properties()
            p.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-scala-application")
            val bootstrapServers = if (args.length > 0) args(0) else "localhost:9092"
            p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            p
        }
        Using(new KafkaStreams(topo("streams-plaintext-input","streams-wordcount-output"), config)){ streams =>
            streams.cleanUp()
            streams.start()
            readLine
        }

    }
}
