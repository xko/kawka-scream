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
    type Follower = User
    type Followee = User

    sealed trait Msg

    case class Connect(user: User) extends Msg

    case class Follow(from: Follower, to: Followee) extends Msg

    case class Unfollow(from: Follower, to: Followee) extends Msg

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


    def sorted(builder: StreamsBuilder, src: KStream[N,Msg]): KStream[N, Msg] = { // TODO: works only with one partition
        val ooo = builder.stream[N, Msg](Topics.ooo)
             .process( Processors.supplier[N, Msg, N, Msg, N, Msg, Delayer](new Delayer(_)) ) // <<< why not inferred?
        val withOrderInfo: KStream[N, (N, Msg)] = src.merge(ooo)
             .process( Processors.supplier[N, Msg, N, (N, Msg), String, N, OrderInformer](new OrderInformer(_)) ) // <<< why not inferred?

        val ordered = withOrderInfo.split().branch({
            case (n, expected -> _) => n != expected
        }, Branched.withConsumer { outOfOrder: KStream[N, (N, Msg)] =>
            outOfOrder.mapValues(_._2).to(Topics.ooo)
        }).defaultBranch().values.head

        ordered.mapValues(_._2)
    }

    def followers(builder: StreamsBuilder, src: KStream[N, Msg]): KTable[Followee, Set[Follower]] = {
        src.filter { (_, msg) => msg match {
            case Follow(_,_) => true
            case Unfollow(_,_) => true
            case _ => false
        }}.groupBy[Followee]{ (_, msg) =>msg match {
            case Follow(_, followee) => followee
            case Unfollow(_, followee) => followee
        }}.aggregate(Set[Follower]()){ (_, msg, followers) => msg match {
                case Follow(follower, _) => followers + follower
                case Unfollow(follower, _) => followers - follower
        }}
    }

    def delivered(builder: StreamsBuilder, src: KStream[N, Msg], followers: KTable[Followee, Set[Follower]]): KStream[User, Msg] = {
        val bySender = src.flatMap[User,Msg] { (_, msg) =>
            msg match {
                case Follow(follower, _) => Some((follower, msg))
                case Update(followee) => Some((followee, msg))
                case _ => None
            }
        }
        val withReceivers: KStream[User, (Msg, Set[User])] = bySender.leftJoin[Set[Follower],(Msg,Set[Follower])](followers) {
            (msg, followers) => (msg, Option(followers).toSet.flatten)
        }
        withReceivers.flatMap { (sender, msgWReceivers) => msgWReceivers match {
            case (update@Update(_), followers)   => followers.map(follower => (follower, update))
            case (follow@Follow(_, followee),_)  => Set((followee, follow))
        }}
    }

}
