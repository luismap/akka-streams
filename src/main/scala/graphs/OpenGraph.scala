package graphs

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, FlowShape, SinkShape, SourceShape}
import akka.stream.scaladsl.{Broadcast, Concat, Flow, GraphDSL, Sink, Source}
import org.apache.log4j.Logger

import scala.concurrent.duration.DurationInt

object OpenGraph extends App {

  /**
   * create a composite source
   * source1 ->
   * merge    -> sourceType
   * source2 ->
   */

  implicit val system = ActorSystem("open-graph")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  val scheduler = system.scheduler
  implicit val execCtx = system.dispatcher
  val logger = Logger.getLogger(this.getClass.getName)
  val log = (data: Any, info: String) => logger.info(s"[$info] $data")


  val source1 = Source(0 to 20)
  val source2 = Source(40 to 60)

  val newSource = Source.fromGraph(
    GraphDSL.create() {
      implicit builder: GraphDSL.Builder[NotUsed] =>
        import GraphDSL.Implicits._
        val concat = builder.add(Concat[Int](2))
        source1 ~> concat.in(0)
        source2 ~> concat.in(1)

        SourceShape(concat.out)

    }
  )

  scheduler.scheduleOnce(0 millis) {
    newSource.to(Sink.foreach(log(_, "2sourceContat1source"))).run()
  }

  /**
   * a sink with two outputs
   *                    -> sink1
   * source -> sinkGraph
   *                   -> sink2
   */
  val sink1 = Sink.foreach(log(_, "sink-one"))
  val sink2 = Sink.foreach(log(_, "sink-two"))

  val sinkGraph = Sink.fromGraph(
    GraphDSL.create() {
      implicit builder: GraphDSL.Builder[NotUsed] =>
        import GraphDSL.Implicits._
        //create components/shapes
        val broadcast = builder.add(Broadcast[Int](2))

        //here we do not convert sink1 to graphShape, because implicits does it for us
        broadcast ~> sink1
        broadcast ~> sink2

        SinkShape(broadcast.in)
    }
  )

  scheduler.scheduleOnce(3 seconds) {
    source1.to(sinkGraph).run()
  }

  /**
   * a graph with two piped flows
   *
   * source ->
   *          |increment -> filter| -> sink
   */

  val complexFlow = Flow.fromGraph(
    GraphDSL.create(){
      implicit builder: GraphDSL.Builder[NotUsed] =>
        import GraphDSL.Implicits._
        //everything in graph operates on shapes
        //create shapes/components
        //we create shapes because no implicits applies to two components
        val increment = builder.add(Flow[Int].map(_ + 1))
        val filter = builder.add(Flow[Int].filter(_ % 2 == 0))

        increment ~> filter

        FlowShape(increment.in,filter.out)
    }
  )

  scheduler.scheduleOnce(6 seconds){
    Source(1 to 40)
      .via(complexFlow)
      .to(Sink.foreach(log(_,"complex-flow")))
      .run()
  }

  scheduler.scheduleOnce(12 seconds) {
    system.terminate()
  }

}
