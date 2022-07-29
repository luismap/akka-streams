import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source

object HelloStream extends App {

  implicit val system = ActorSystem("hello")
  implicit val materializer = ActorMaterializer()

  Source.single("Hello world").runForeach(println)

  Thread.sleep(1000)
  system.terminate()
}