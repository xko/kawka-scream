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

    class OrderInformer(storeName: String) extends ContextualProcessor[N, Msg, N, (N, Msg)] {
        private var store: KeyValueStore[String, N] = _;

        override def init(context: ProcessorContext[N, (N, Msg)]): Unit = {
            super.init(context)
            store = context.getStateStore(storeName)
        }

        override def process(record: api.Record[N, Msg]): Unit = {
            val expected: N = Option[N](store.get("expected-next")).getOrElse(1)
            if (record.key() == expected) store.put("expected-next", expected + 1)
            context().forward(record.withValue((expected ,record.value)))
        }
    }

    class Delayer(storeName: String) extends ContextualProcessor[N, Msg, N, Msg] with LazyLogging{
        private var store: KeyValueStore[N, Msg] = _;

        override def init(context: ProcessorContext[N, Msg]): Unit = {
            super.init(context)
            store = context.getStateStore(storeName)
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

    def memoryStoredProc[K,V,RK,RV,SK : Serde,SV : Serde](proc: String => Processor[K,V,RK,RV], name:String = java.util.UUID.randomUUID.toString ) = { new ProcessorSupplier[K,V,RK,RV] {
        override def get(): Processor[K,V,RK,RV]= proc("store-" + name)

        override def stores(): util.Set[StoreBuilder[_]] = {
            Collections.singleton(Stores.keyValueStoreBuilder[SK, SV](Stores.inMemoryKeyValueStore("store-"+name), implicitly[Serde[SK]], implicitly[Serde[SV]]))
        }

    } }

    def sorted(builder: StreamsBuilder): KStream[N,Msg] = {
        val src = builder.stream[N, Msg](Topics.source)
        val ooo = builder.stream[N, Msg](Topics.ooo).process( memoryStoredProc[N,Msg,N,Msg,N,Msg](new Delayer(_)) )
        val withOrderInfo: KStream[N, (N, Msg)] = src.merge(ooo).process( memoryStoredProc[N,Msg,N,(N,Msg),String,N](new OrderInformer(_)  ))

        val m = withOrderInfo.split().branch({
            case (n, expected -> _) => n != expected
        }, Branched.withConsumer { outOfOrder: KStream[N, (N, Msg)] =>
            outOfOrder.mapValues(_._2).to(Topics.ooo)
        }).defaultBranch()
        m.values.head.mapValues(_._2)
    }




}
