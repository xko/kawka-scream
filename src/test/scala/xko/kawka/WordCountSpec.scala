package xko.kawka

import org.apache.kafka.common.serialization.{LongDeserializer, StringDeserializer, StringSerializer}
import org.apache.kafka.streams.{KeyValue, StreamsConfig, TopologyTestDriver}
import org.apache.kafka.test.TestUtils
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.serialization.Serdes._

import java.util.Properties
import scala.jdk.CollectionConverters._

class WordCountSpec extends AnyWordSpec with Matchers with TestConfig {

    "WordCount" should {
        "count words" in {
            val inputTopic = "inputTopic"
            val outputTopic = "output-topic"
            val td = new TopologyTestDriver(WordCount.topo(inputTopic,outputTopic), config)
            val from = td.createInputTopic(inputTopic, new StringSerializer, new StringSerializer)
            val to = td.createOutputTopic(outputTopic, new StringDeserializer, new LongDeserializer)
            val input = Seq("Hello Kafka Streams", "All streams lead to Kafka", "Join Kafka Summit").map(new KeyValue("", _))
            from.pipeKeyValueList(input.asJava)
            val output = to.readKeyValuesToMap().asScala
            output should contain theSameElementsAs( Seq(
                "all" -> 1, "kafka" -> 3, "streams" -> 2, "hello" -> 1, "to" -> 1, "join" -> 1, "summit" -> 1, "lead" -> 1
            ) )
        }

    }
}
