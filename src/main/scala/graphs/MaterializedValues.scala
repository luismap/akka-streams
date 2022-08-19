package graphs

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, FlowShape, SinkShape}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, RunnableGraph, Sink, Source}
import org.agrona.concurrent.broadcast.BroadcastBufferDescriptor
import org.apache.log4j.Logger

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

object MaterializedValues extends App {

  implicit val system = ActorSystem("system")
  implicit val materializer = ActorMaterializer()
  val scheduler = system.scheduler
  implicit val execCtx = system.dispatcher
  val logger = Logger.getLogger(this.getClass.getName)

  def log = (data: Any, info: String) => logger.info(s"[$info] $data")

  val words = "Hello world in this place Of Incoms".split("\\s+")
  val wordSource = Source(words.toList)
  val printer = Sink.foreach(log(_, "printing word"))
  val counter = Sink.fold[Int, String](0)((a, _) => a + 1)
  val counterLower = Sink.fold[Int, String](0)((a, _) => a + 1)

  /**
   * composite sink with materialized value
   * #words ->
   * #        broadcast -> filter -> printer
   * #                  -> filter -> counter
   */

  val complexSink = Sink.fromGraph(
    //pass the component you want to materialize
    GraphDSL.create(counter) {
      //the function become a higher order func with the value of the component
      //and we use that value in place of the original component
      implicit builder: GraphDSL.Builder[Future[Int]] =>
        counterShape =>
          import GraphDSL.Implicits._
          //create components
          val broadcast = builder.add(Broadcast[String](2))
          val lowerCase = builder.add(Flow[String].filter(a => a.toLowerCase == a))
          val shorter5char = builder.add(Flow[String].filter(_.length < 5))
          //connect components
          broadcast ~> lowerCase ~> printer
          broadcast ~> shorter5char ~> counterShape

          //define shape(how external components will interact)
          SinkShape(broadcast.in)
    }
  )

  val complexSinkTwoMatVal = Sink.fromGraph(
    //pass the components you want to materialize
    //and addition a combiner function
    GraphDSL.create(counter, counterLower)((counter, counterLower) => (counter, counterLower)) {
      //the function become a higher order func with the value of the component
      //and we use that value in place of the original component
      implicit builder: GraphDSL.Builder[(Future[Int], Future[Int])] =>
        (counterShape, printerShape) =>
          import GraphDSL.Implicits._
          //create components
          val broadcast = builder.add(Broadcast[String](3))
          val lowerCase = builder.add(Flow[String].filter(a => a.toLowerCase == a))
          val lowerCase2 = builder.add(Flow[String].filter(a => a.toLowerCase == a))
          val shorter5char = builder.add(Flow[String].filter(_.length < 5))
          //connect components
          broadcast ~> lowerCase ~> printer
          broadcast ~> shorter5char ~> counterShape
          broadcast ~> lowerCase2 ~> printerShape

          //define shape(how external components will interact)
          SinkShape(broadcast.in)
    }
  )

  scheduler.scheduleOnce(0 millis) {
    val countedShorter5char = wordSource.toMat(complexSink)(Keep.right).run()
    countedShorter5char.onComplete {
      case Success(data) => log(data, "less than 5 character count")
      case Failure(exception) => log(exception.getMessage, "got exception")
    }
  }

  scheduler.scheduleOnce(3 seconds) {
    val countedShorter5char2 = wordSource.toMat(complexSinkTwoMatVal)(Keep.right).run()
    countedShorter5char2._1.onComplete {
      case Success(data) => log(data, "less than 5 character count")
      case Failure(exception) => log(exception.getMessage, "got exception")
    }
    countedShorter5char2._2.onComplete {
      case Success(data) => log(data, "lower case character count")
      case Failure(exception) => log(exception.getMessage, "got exception")
    }
  }


  /**
   * challenge:
   * create enhance flow that
   * receives a flow and return same flow with a materialize count of numbers of elements
   * that pass through the flow
   *
   * #flow[A,B,_] => flow[A,B,Future[Int]]
   *
   * example pipeline
   * #source ->
   * #        enhanceflow -> sink
   */

  def enhanceFlow[A, B](flow: Flow[A, B, _]): Flow[A, B, Future[Int]] = {
    val counteri = Sink.fold[Int, B](0)((a, _) => a + 1)
    Flow.fromGraph(
      GraphDSL.create(counteri) {
        implicit builder: GraphDSL.Builder[Future[Int]] => counterShape =>
          import GraphDSL.Implicits._
          //create componenents
          val broadcast = builder.add(Broadcast[A](2))
          val iflow = builder.add(flow)
          val bflow = builder.add(flow)
          //connect
           broadcast ~> iflow.in
           iflow ~> counterShape
           broadcast.out(1) ~> bflow.in

          FlowShape(broadcast.in, bflow.out)

      }
    ).asInstanceOf[Flow[A,B,Future[Int]]]
  }

  scheduler.scheduleOnce(9 seconds) {
    val flow = Flow[String].filter(_.contains("o"))
    val countedShorter5char2 = wordSource.viaMat(enhanceFlow(flow))(Keep.right).to(printer).run()
    countedShorter5char2.onComplete {
      case Success(data) => log(data, "count of element through flow")
      case Failure(exception) => log(exception.getMessage, "got exception")
    }
  }


  scheduler.scheduleOnce(12 seconds) {
    system.terminate()
  }
}
