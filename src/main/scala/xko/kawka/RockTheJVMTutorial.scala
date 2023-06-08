package xko.kawka

import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import io.circe.{Decoder, Encoder}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{GlobalKTable, JoinWindows, TimeWindows, Windowed}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.{KGroupedStream, KStream, KTable}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

import java.util.Properties
import scala.concurrent.duration._
import scala.io.StdIn.readLine
import scala.jdk.DurationConverters.ScalaDurationOps
import scala.util.Using

// https://blog.rockthejvm.com/kafka-streams/
object RockTheJVMTutorial {
    final val OrdersByUserTopic = "orders-by-user"
    final val DiscountProfilesByUserTopic = "discount-profiles-by-user"
    final val DiscountsTopic = "discounts"
    final val OrdersTopic = "orders"
    final val PaymentsTopic = "payments"
    final val PaidOrdersTopic = "paid-orders"

    type UserId = String
    type Profile = String
    type Product = String
    type OrderId = String

    case class Order(orderId: OrderId, user: UserId, products: List[Product], amount: Double)

    case class Discount(profile: Profile, amount: Double) // in percentage points

    case class Payment(orderId: OrderId, status: String)

    implicit def serde[A >: Null : Decoder : Encoder]: Serde[A] = {
        val serializer = (a: A) => a.asJson.noSpaces.getBytes
        val deserializer = (aAsBytes: Array[Byte]) => {
            val aAsString = new String(aAsBytes)
            val aOrError = decode[A](aAsString)
            aOrError match {
                case Right(a) => Option(a)
                case Left(error) =>
                    println(s"There was an error converting the message $aOrError, $error")
                    Option.empty
            }
        }
        Serdes.fromFn[A](serializer, deserializer)
    }

    val builder = new StreamsBuilder
    val usersOrdersStreams: KStream[UserId, Order] = builder.stream[UserId, Order](OrdersByUserTopic)
    val userProfilesTable: KTable[UserId, Profile] = builder.table[UserId, Profile](DiscountProfilesByUserTopic)
    val discountProfilesGTable: GlobalKTable[Profile, Discount] = builder.globalTable[Profile, Discount](DiscountsTopic)

    val expensiveOrders: KStream[UserId, Order] = usersOrdersStreams.filter { (userId, order) =>
        order.amount >= 1000
    }
    val purchasedListOfProductsStream: KStream[UserId, List[Product]] = usersOrdersStreams.mapValues { order =>
        order.products
    }
    val purchasedProductsStream: KStream[UserId, Product] = usersOrdersStreams.flatMapValues { order =>
        order.products
    }
    val productsPurchasedByUsers: KGroupedStream[UserId, Product] = purchasedProductsStream.groupByKey

    val purchasedByFirstLetter: KGroupedStream[String, Product] = purchasedProductsStream.groupBy[String] { (userId, _) =>
        userId.charAt(0).toLower.toString
    }
    val numberOfProductsByUser: KTable[UserId, Long] = productsPurchasedByUsers.count()

    val everyTenSeconds: TimeWindows = TimeWindows.of(10.second.toJava)

    val numberOfProductsByUserEveryTenSeconds: KTable[Windowed[UserId], Long] =
        productsPurchasedByUsers.windowedBy(everyTenSeconds).aggregate[Long](0L) { (_, _, counter) =>
            counter + 1
        }

    val ordersWithUserProfileStream: KStream[UserId, (Order, Profile)] =
        usersOrdersStreams.join[Profile, (Order, Profile)](userProfilesTable) { (order, profile) =>
            (order, profile)
        }

    val discountedOrdersStream: KStream[UserId, Order] =
        ordersWithUserProfileStream.join[Profile, Discount, Order](discountProfilesGTable)(
            { case (_, (_, profile)) => profile }, // Joining key
            { case ((order, _), discount) => order.copy(amount = order.amount * discount.amount) }
        )

    val paymentsStream: KStream[OrderId, Payment] = builder.stream[OrderId, Payment](PaymentsTopic)

    val ordersStream: KStream[OrderId, Order] = discountedOrdersStream.selectKey { (_, order) => order.orderId }

    val paidOrders: KStream[OrderId, Order] = {
        val joinOrdersAndPayments = (order: Order, payment: Payment) => Option.when(payment.status == "PAID")(order)
        val joinWindow = JoinWindows.of(5.minutes.toJava)

        ordersStream.join[Payment, Option[Order]](paymentsStream)(joinOrdersAndPayments, joinWindow)
                    .flatMapValues(maybeOrder => maybeOrder.toIterable)
    }

    def main(args: Array[String]): Unit = {
        val props = new Properties
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-application")
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)

        paidOrders.to(PaidOrdersTopic)

        val topology: Topology = builder.build()

        println(topology.describe())

        Using(new KafkaStreams(topology, props)){ streams =>
            streams.start()
            readLine
        }


    }
}