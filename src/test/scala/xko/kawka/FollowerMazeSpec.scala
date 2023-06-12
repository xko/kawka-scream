package xko.kawka

import io.circe.generic.auto._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import xko.kawka.FollowerMaze.{Connect, Follow, Msg, N, Unfollow, Update}

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

    "Implementation" should {
        "restore ordering" in {
            val builder = new StreamsBuilder
            FollowerMaze.sorted(builder,builder.stream("sort-src")).to("sort-sink")
            val td = new TopologyTestDriver(builder.build, config)
            val from = td.createInputTopic("sort-src", implicitly[Serde[Long]].serializer(), implicitly[Serde[Msg]].serializer())
            val to = td.createOutputTopic("sort-sink", implicitly[Serde[Long]].deserializer(), implicitly[Serde[Msg]].deserializer())
            val ns: Seq[Long] = List(1L, 2, 5, 4, 3, 6, 7, 8, 9)
            ns.foreach { i =>
                from.pipeInput(i, Connect(i).asInstanceOf[Msg])
            }
            td.advanceWallClockTime(10.milli.toJava)
            val output = to.readKeyValuesToMap().asScala
            output.keys.toSeq should contain theSameElementsInOrderAs  ns.sorted
        }

       "keep followers table" in {
           val builder = new StreamsBuilder
           FollowerMaze.followers(builder.stream("f-src")).toStream.to("f-sink")
           val td = new TopologyTestDriver(builder.build, config)
           val from = td.createInputTopic("f-src", implicitly[Serde[Long]].serializer(), implicitly[Serde[Msg]].serializer())
           val to = td.createOutputTopic("f-sink", implicitly[Serde[Long]].deserializer(), implicitly[Serde[Set[Long]]].deserializer())
           from.pipeInput(1, Follow(1,2))
           from.pipeInput(2, Follow(1,3))
           from.pipeInput(3, Follow(4,2))
           from.pipeInput(4, Follow(2,5))
           from.pipeInput(5, Follow(4,5))
           from.pipeInput(1, Follow(3,5))
           from.pipeInput(1, Unfollow(2,5))
           val out = to.readKeyValuesToMap().asScala
           out should contain theSameElementsAs Seq(
               2 -> Set(1,4),
               3 -> Set(1),
               5 -> Set(3,4)
           )
       }

       "deliver in order" in {
           val builder = new StreamsBuilder
           val src = builder.stream[N,Msg]("f-src")
           FollowerMaze.delivered(src,FollowerMaze.followers(src)).to("f-sink")
           val td = new TopologyTestDriver(builder.build, config)
           val from = td.createInputTopic("f-src", implicitly[Serde[Long]].serializer(), implicitly[Serde[Msg]].serializer())
           val to = td.createOutputTopic("f-sink", implicitly[Serde[Long]].deserializer(), implicitly[Serde[Msg]].deserializer())
           from.pipeInput(1, Follow(1,2))
           from.pipeInput(2, Follow(1,3))
           from.pipeInput(3, Follow(4,2))
           from.pipeInput(3, Update(2))
           from.pipeInput(4, Follow(2,5))
           from.pipeInput(4, Update(5))
           from.pipeInput(5, Follow(4,5))
           from.pipeInput(1, Follow(3,5))
           from.pipeInput(1, Unfollow(2,5))
           from.pipeInput(4, Update(5))
           val out = to.readKeyValuesToList().asScala.map(kv => kv.key -> kv.value)
           out.toSeq shouldBe  Seq(
               2 -> Follow(1,2),
               3 -> Follow(1,3),
               2 -> Follow(4,2),
               1 -> Update(2),
               4 -> Update(2),
               5 -> Follow(2,5),
               2 -> Update(5),
               5 -> Follow(4,5),
               5 -> Follow(3,5),
               4 -> Update(5),
               3 -> Update(5),
           )
       }
    }

}
