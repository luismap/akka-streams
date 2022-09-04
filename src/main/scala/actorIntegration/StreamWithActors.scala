package actorIntegration

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.Timeout
import org.apache.log4j.Logger

import scala.concurrent.duration.DurationInt


object StreamWithActors extends App {
  class HandlerActor extends Actor with ActorLogging {
    override def receive: Receive = {
      case s => log.info(s"[handled] $s")
    }
  }

  class SimpleActor extends Actor with ActorLogging {
    override def receive: Receive = {
      case s: String => log.info(s"[received string] $s")
      case n: Int =>
        log.info(s"[received number] $n")
        sender() ! n * 2
    }
  }

  implicit val system = ActorSystem("system")
  val ha = system.actorOf(Props[HandlerActor])
  implicit val materializer = ActorMaterializer()
  val scheduler = system.scheduler
  implicit val execCtx = system.dispatcher
  implicit val timeout = Timeout(500 millis)
  val logger = Logger.getLogger(this.getClass.getName)
  val log = (x: Any, info: String) => logger.info(s"[$info] $x")

  val sa = system.actorOf(Props[SimpleActor], "sa")

  //scheduler.scheduleOnce(0 millis) {(0 to 10) foreach (sa.tell(_,ha))}

  scheduler.scheduleOnce(0 millis) {
    /**
     * actors interact with stream using the ask pattern
     * - it will receive futures
     * - parallelism (how many request in flight)
     */

    val source = Source(1 to 10)
    val flow = Flow[Int].ask[Int](parallelism = 4)(sa)

    source.via(flow).to(Sink.ignore).run()

  }

  scheduler.scheduleOnce(1 seconds){
    val source = Source(20 to 30)
    //shorter way of writing the ask above, it create a similar flow
    source.ask[Int](sa).to(Sink.ignore).run()
  }

  scheduler.scheduleOnce( 2 seconds) {
    logger.info("\n")

    /**
     * Actor as a source
     * - creates an actor that will pass the messages through
     */
    val actorSource: Source[Int, ActorRef] = Source.actorRef[Int](10, OverflowStrategy.dropHead)
    val materializeActorRef = actorSource.to(Sink.foreach(log(_, "sinking actor source"))).run()

    (0 to 10).foreach( materializeActorRef ! _)

  }

  scheduler.scheduleOnce(4 seconds)  {
    logger.info("\n")

    /**
     * actor as a destination/sink
     * it needs to handle the following messages:
     * - init
     * - ack
     * - complete
     * - function for exception handling
     */
    object InitStream
    object AckStream
    object CompleteStream
    case class FailureStream(ex: Throwable)


    class SinkActor2 extends Actor with ActorLogging {
      override def receive: Receive = {
        case InitStream =>
          log.info(s"[stream] initialized")
          sender() ! AckStream //you need to ack
        case CompleteStream =>
          log.info(s"[stream] completed")
          context.stop(self)
        case FailureStream(exception) =>
          log.warning(s"[stream] failed with: $exception")
        case message =>
          log.info(s"[stream] got message $message")
          sender() ! AckStream //need to ack, otherwise backpressure triggers
      }
    }

    //need to user Props(new ..), not working otherwise
    val sinkActor = system.actorOf(Props(new SinkActor2))
    val sink = Sink.actorRefWithAck[Int](
      sinkActor,
      onInitMessage = InitStream,
      onCompleteMessage = CompleteStream,
      ackMessage = AckStream,
      onFailureMessage = throwable => FailureStream(throwable)
    )

    Source(100 to 110).to(sink).run()

  }


  scheduler.scheduleOnce(12 seconds) {
    system.terminate()
  }
1 
}
