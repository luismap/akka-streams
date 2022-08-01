package intro

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import org.apache.log4j.Logger

import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

object MaterializingStreams extends App {

  /**
   * (i) Materializing
   * - each node produces a materialize view when run
   * - the graph produces a single materialize view
   * - by default graph keeps the left value
   * - you could tweak which value will the graph produce at the end
   * - a node can materialize multiple times
   * - materializing a graph = to materialize all nodes
   */

  implicit val system = ActorSystem("materializing-streams")
  implicit val materiliazer = ActorMaterializer()
  val log = Logger.getLogger(this.getClass.getName)
  val scheduler = system.scheduler
  implicit val execCtx = system.dispatcher

  def log(msg: Any): Unit = log.info(s"[logging-msg] ${msg.toString}")


  val source = Source(Stream.from(1).take(10))
  val sink = Sink.reduce[Int]((a, b) => a + b) //reduce returns future

  scheduler.scheduleOnce(0 millis) {
    source.runWith(sink).onComplete {
      case Success(value) => log.info(s"[sum of elemenst] $value")
      case Failure(exception) => log.info(s"[failure] ${exception.getMessage}")
    }
  }

  /**
   * a graph will only keep by default the leftmost materialize value
   * ex. node(Mat) -> node2(Mat) -> nodeN(Mat)
   * to change behaviour, you can use methods xMat(Keep[both, left, right])
   * to keep the materialize value you want
   */
  scheduler.scheduleOnce(1 seconds) {
    val source = Source(1 to 10)
    val mapper = Flow[Int].map(_ + 1)
    val sink = Sink.foreach((a: Any) => log.info(a))
    val graph = source
      .viaMat(mapper)((_, matb) => matb)
      .toMat(sink)((mata, matb) => matb)


    val graphWithKeep = source
      .viaMat(mapper)(Keep.right)
      .toMat(sink)(Keep.right)

    graph.run().onComplete{
      case Success(value) => log.info(s"[successfull graph] $value")
      case Failure(exception) => log.info(s"[graph failure] ${exception.getMessage}")
    }

   // graphWithKeep.run()
  }


  scheduler.scheduleOnce(12 seconds) {
    system.terminate()
  }

}
