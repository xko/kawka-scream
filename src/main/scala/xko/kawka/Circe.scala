package xko.kawka

import io.circe.parser._
import io.circe.syntax._
import io.circe.{Decoder, Encoder}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.serialization.Serdes

object Circe {
    implicit def serde[A >: Null : Decoder : Encoder]: Serde[A] = {
        val serializer = (a: A) =>
            a.asJson.noSpaces.getBytes
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


}
