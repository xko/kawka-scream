package xko.kawka

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
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.streams.processor.{PunctuationType, Punctuator, api}
import org.apache.kafka.streams.processor.api.{ContextualProcessor, Processor, ProcessorContext, ProcessorSupplier}
import org.apache.kafka.streams.state.{KeyValueStore, StoreBuilder, Stores}
import xko.kawka.Processors.{StoreProvider, WithStore, memStore}

import java.util
import scala.jdk.CollectionConverters._
import scala.jdk.DurationConverters._
import scala.concurrent.duration._


object FollowerMaze {
    type N = Long
    type User = Long

    sealed trait Msg

    case class Connect(user: User) extends Msg

    case class Follow(from: User, to: User) extends Msg

    case class Unfollow(from: User, to: User) extends Msg

//   TODO case class Broadcast() extends Msg

    case class Update(from: User) extends Msg


    object Topics {
        val source = "source"
        val sink = "sink"
        val ooo = "ooo"

    }

    class OrderInformer(storeName: String) extends WithStore[N, Msg, N, (N, Msg), String, N](storeName) {
        override def process(record: api.Record[N, Msg]): Unit = {
            val expected: N = Option[N](store.get("expected-next")).getOrElse(1)
            if (record.key() == expected) store.put("expected-next", expected + 1)
            context().forward(record.withValue((expected, record.value)))
        }
    }

    implicit val orderInformerInMem = new StoreProvider[String, N, OrderInformer] {
        override def provide(name: String) = memStore(name)
    }

    class Delayer(storeName: String) extends WithStore[N, Msg, N, Msg,N,Msg](storeName) with LazyLogging {
        override def init(context: ProcessorContext[N, Msg]): Unit = {
            super.init(context)
            context.schedule(10.milli.toJava, PunctuationType.WALL_CLOCK_TIME, (timestamp: Long) => {
                logger.info(s"Resending...")
                store.all().asScala.foreach { kv =>
                    logger.info(s"Resend:$kv")
                    context.forward(new api.Record[N, Msg](kv.key, kv.value, timestamp))
                    store.delete(kv.key)
                }
            })
        }
        override def process(record: api.Record[N, Msg]): Unit = {
            logger.info(s"Process:$record")
            store.put(record.key, record.value)
        }
    }

    implicit val delayerInMem = new StoreProvider[N,Msg,Delayer] {
        override def provide(name: String): StoreBuilder[KeyValueStore[N, Msg]] = memStore(name)
    }


    def sorted(builder: StreamsBuilder): KStream[N, Msg] = {
        val src = builder.stream[N, Msg](Topics.source)
        val ooo = builder.stream[N, Msg](Topics.ooo)
             .process( Processors.supplier[N, Msg, N, Msg, N, Msg, Delayer](new Delayer(_)) ) // <<< why not inferred?
        val withOrderInfo: KStream[N, (N, Msg)] = src.merge(ooo)
             .process( Processors.supplier[N, Msg, N, (N, Msg), String, N, OrderInformer](new OrderInformer(_)) ) // <<< why not inferred?

        val m = withOrderInfo.split().branch({
            case (n, expected -> _) => n != expected
        }, Branched.withConsumer { outOfOrder: KStream[N, (N, Msg)] =>
            outOfOrder.mapValues(_._2).to(Topics.ooo)
        }).defaultBranch()
        m.values.head.mapValues(_._2)
    }


}
