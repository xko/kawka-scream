package xko.kawka

import io.circe.generic.auto._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import xko.kawka.FollowerMaze.{Connect, Msg}

import scala.jdk.CollectionConverters._
import scala.jdk.DurationConverters._
import scala.concurrent.duration._
import org.apache.kafka.streams.scala.StreamsBuilder
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import io.circe.{Decoder, Encoder}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{GlobalKTable, JoinWindows, TimeWindows, Windowed}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.{Branched, BranchedKStream, KGroupedStream, KStream, KTable}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

import java.util.{Collections, Properties}
import scala.concurrent.duration._
import scala.io.StdIn.readLine
import scala.jdk.DurationConverters.ScalaDurationOps
import scala.util.Using
import Circe._
import org.apache.kafka.streams.processor.{PunctuationType, Punctuator, api}
import org.apache.kafka.streams.processor.api.{ContextualProcessor, Processor, ProcessorContext, ProcessorSupplier}
import org.apache.kafka.streams.state.{KeyValueStore, StoreBuilder, Stores}

import java.util
import scala.jdk.CollectionConverters._
import scala.jdk.DurationConverters._
import scala.concurrent.duration._

class FollowerMazeSpec extends AnyWordSpec with Matchers with TestConfig {
    import Circe._

    "Ordering" should {
        "be restored" in {
            val builder = new StreamsBuilder
            FollowerMaze.sorted(builder).to("sortd-sink")
            val td = new TopologyTestDriver(builder.build, config)
            val from = td.createInputTopic(FollowerMaze.Topics.source, implicitly[Serde[Long]].serializer(), implicitly[Serde[Msg]].serializer())
            val to = td.createOutputTopic("sortd-sink", implicitly[Serde[Long]].deserializer(), implicitly[Serde[Msg]].deserializer())
            val ns: Seq[Long] = List(1L, 2, 5, 4, 3, 6, 7, 8, 9)
            ns.foreach { i =>
                from.pipeInput(i, Connect(i).asInstanceOf[Msg])
                from.advanceTime(1.milli.toJava)
            }
            td.advanceWallClockTime(10.milli.toJava)
            val output = to.readKeyValuesToMap().asScala
            output.keys.toSeq should contain theSameElementsInOrderAs  ns.sorted


        }
    }


}
