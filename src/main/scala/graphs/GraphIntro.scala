package graphs

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape}
import akka.stream.scaladsl.{Balance, Broadcast, Flow, GraphDSL, Merge, RunnableGraph, Sink, Source, Zip}
import org.apache.log4j.Logger

import scala.concurrent.duration.DurationInt

object GraphIntro extends App {

  /**
   * to create advance graph, ex. fan-in, fan-out
   * create your own runnable graph
   */

  implicit val system = ActorSystem("system")
  implicit val materializer = ActorMaterializer()
  implicit val exCtx = system.dispatcher
  val scheduler = system.scheduler
  val logger = Logger.getLogger(this.getClass.getName)

  def log(l: Any, info: String) = logger.info(s"[logging] $info ${l.toString}")

  val source = Source(1 to 10)
  val increment = Flow[Int].map(_ + 1)
  val multiplier = Flow[Int].map(_ * 2)
  val sink = Sink.foreach[(Int, Int)](log(_, "two flow graph"))


  //create graph
  val graph = RunnableGraph.fromGraph(
    GraphDSL.create() {
      implicit builder: GraphDSL.Builder[NotUsed] => {
        import GraphDSL.Implicits._

        //building graph
        val broadcast = builder.add(Broadcast[Int](2)) //fan-out operator
        val zip = builder.add(Zip[Int, Int]) //fan-in operator

        //connecting components
        source ~> broadcast
        broadcast.out(0) ~> increment ~> zip.in0
        broadcast.out(1) ~> multiplier ~> zip.in1
        zip.out ~> sink

        //
        ClosedShape //freeze the shape
      }
    }
  )

  graph.run()

  /**
   * ex. 1 source into two sinks
   */
  val source2 = Source(1 to 10)
  val sink2 = Sink.foreach(log(_, "sink one"))
  val sink3 = Sink.foreach(log(_, "sink two"))

  val graphTwoSinks = RunnableGraph.fromGraph(
    GraphDSL.create() {
      implicit builder: GraphDSL.Builder[NotUsed] =>
        import GraphDSL.Implicits._
        //defining graph
        val broadcast = builder.add(Broadcast[Int](2))

        //connecting graph
        source2 ~> broadcast
        broadcast.out(0) ~> sink2
        broadcast.out(1) ~> sink3

        //freeze graph
        ClosedShape
    }
  )

  scheduler.scheduleOnce(3 seconds) {
    graphTwoSinks.run()
  }


  /**
   * creating the following graph
   *  slow ->          ->          -> sink A
   *          |merge|     |balance|
   *  fast ->         ->           -> sink B
   */
  val slowSource = Source(1 to 20).map({
    e =>
      Thread.sleep(500)
      e
  })
  val fastSource = Source(1 to 30)

  val sinkA = Sink.foreach(log(_, "sink A"))
  val sinkB = Sink.foreach(log(_, "sink B"))

  val throttleGraph = RunnableGraph.fromGraph(
    GraphDSL.create() {
      implicit builder: GraphDSL.Builder[NotUsed] =>
        import GraphDSL.Implicits._
        //create components
        val merger = builder.add(Merge[Int](2))
        val balancer = builder.add(Balance[Int](2))

        //connect components

        slowSource ~> merger.in(0)
        fastSource ~> merger.in(1)
        merger.out ~> balancer.in

        balancer.out(0) ~> sinkA
        balancer.out(1) ~> sinkB

        //freeze graph
        ClosedShape
    }
  )

  scheduler.scheduleOnce(6 seconds) {
    throttleGraph.run()
  }

  scheduler.scheduleOnce(12 seconds) {
    system.terminate()
  }

}
