import xko.kawka.FollowerMaze._
import io.circe.parser._
import io.circe.syntax._
import io.circe.generic.auto._



val json = Connect(1).asInstanceOf[Msg].asJson.noSpaces
decode[Msg](json)