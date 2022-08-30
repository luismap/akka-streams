package graphs

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape, FanInShape, FanInShape2, FanInShape3, FlowShape, OverflowStrategy, SourceShape, UniformFanInShape, scaladsl}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Merge, MergePreferred, RunnableGraph, Sink, Source, ZipWith}
import jdk.jshell.ImportSnippet
import org.apache.log4j.Logger

import scala.concurrent.duration.DurationInt

object GraphCycles extends App {

  implicit val system = ActorSystem("system")
  implicit val materializer = ActorMaterializer()
  val scheduler = system.scheduler
  val logger = Logger.getLogger(this.getClass.getName)
  val log = (e: Any, info: String) => logger.info(s"[$info] $e")
  implicit val execCtx = system.dispatcher

  /**
   * Adding cyclce to graph can create deadlocks
   * a deadlock graph cycle
   * - because merge will directly start buffer,
   * ex.
   * #source ->
   * #       ->  merger -> flow
   * |                 |
   * |  <-  <- <-   <-
   */

  val accelarator = GraphDSL.create() {
    implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._
      val source = builder.add(Source(1 to 20))
      val merger = builder.add(Merge[Int](2))
      val flow = builder.add(Flow[Int].map {
        e =>
          log(e, "accelerating")
          e + 1
      })

      source ~> merger ~> flow
      merger <~ flow.out

      ClosedShape
  }

  //RunnableGraph.fromGraph(accelarator).run() // will only print first element

  /**
   * fixing deadlock from above
   * using MergePreferred
   */

  val accelaratorPref = GraphDSL.create() {
    implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._
      val source = builder.add(Source(1 to 20))
      val merger = builder.add(MergePreferred[Int](1))
      val flow = builder.add(Flow[Int].map {
        e =>
          log(e, "accelerating with mergePreferred")
          e + 1
      })
      val stop = builder.add(Flow[Int].takeWhile(_ < 200))

      source ~> merger ~> flow ~> stop
      merger.preferred <~ stop.out

      ClosedShape
  }

  //scheduler.scheduleOnce(0 millis) {RunnableGraph.fromGraph(accelaratorPref).run()}

  /**
   * fix deadlocks using buffers
   */

  val accelaratorBuffers = GraphDSL.create() {
    implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._
      val source = builder.add(Source(1 to 10))
      val sourceLogger = builder.add(Flow[Int].map {
        e =>
          log(e, "source value")
          e
      })
      val merger = builder.add(Merge[Int](2))
      val flow = builder.add(Flow[Int].buffer(10, OverflowStrategy.dropTail).map {
        e =>
          Thread.sleep(100)
          log(e, "accelerating with buffers")
          e + 1
      })
      val stop = builder.add(Flow[Int].takeWhile(_ < 10))

      source ~> sourceLogger ~> merger ~> flow ~> stop
      merger <~ stop.out

      ClosedShape
  }

  //scheduler.scheduleOnce(3 seconds) {RunnableGraph.fromGraph(accelaratorBuffers).run()}

  /**
   * fib graph
   * #  init(1,1) ->
   * #              mergePreferred -> computFib
   * #              mergePreferrred <- computeFib
   * #
   */

  val fibFanInShape = GraphDSL.create() {
    implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._

      val mergePreferred = builder.add(MergePreferred[(Int,Int)](1))
      val broadcast = builder.add(Broadcast[(Int, Int)](2))
      val fib = builder.add(Flow[(Int, Int)].map { case (a, b) =>
        val ans = (b, a + b)
        log((a, b), s"${ans._2} is fib of")
        Thread.sleep(100)
        ans
      })

      val stop = builder.add(Flow[(Int,Int)].takeWhile(_._2 < 2000))

      mergePreferred ~> fib.in
      fib.out ~> stop ~> broadcast
      broadcast.out(1) ~> mergePreferred.preferred

       UniformFanInShape(broadcast.out(0), mergePreferred.in(0))
  }
  val fibGraph = GraphDSL.create() {
    implicit builder: GraphDSL.Builder[NotUsed] =>
      val fibFanIn = builder.add(fibFanInShape)
      val init = builder.add(Source.single((1,1)))
      import GraphDSL.Implicits._

      init.out ~> fibFanIn ~> Sink.foreach(null) //you always need to sink the graph

      ClosedShape

  }

  RunnableGraph.fromGraph(fibGraph).run()


  scheduler.scheduleOnce(3 seconds) {
    system.terminate()
  }
}
