package intro

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Sink, Source}
import org.apache.log4j.Logger

import scala.concurrent.duration.DurationInt

object BackPressure extends App {

  implicit val system = ActorSystem("system")
  implicit val materializer = ActorMaterializer()
  val logger = Logger.getLogger(this.getClass.getName)
  val scheduler = system.scheduler
  implicit val execCtx = system.dispatcher

  val source = Source(1 to 32)
  val flow = Flow[Int].map((x: Int) => {logger.info(s"Processing: $x"); x  + 1})
  val slowSync = Sink.foreach( (x: Int) => {Thread.sleep(100); logger.info(s"Sinking $x")})

  /**
   * about backpressure
   * - akka stream implements back pressure by default
   * - even when operator fusion is in place, backpressure is in place
   * - when a component is slow, sends backpressure signal to parent
   * - when backpressure signal received, components buffers 15 message by default
   */

  source.async
    .via(flow).async
    .to(slowSync)
    .run()


  /**
   * source.async //received BP signal
    .via(inc).async // received BP signal, starts to buffer
    .to(slowSync) //sends BP signal
    .run()
   */


  /**
   * reactions to backpressure (BP) in order
   * - slow down component (usually when operator fusion in place)
   * - buffer elements
   * - drop elements from buffer if overflow
   * - kill stream
   */

    //can tweak buffer behaviour by size, and strategies
  val bufferedFlow = flow.buffer(10, OverflowStrategy.dropHead )

  scheduler.scheduleOnce(5 seconds) {
    Source(1 to 64).async
      .via(bufferedFlow).async
      .to(slowSync)
      .run()
  }

  //throttling
  //useful for controlling rates
  val throttleSource = source.throttle(10, 1 seconds)

  scheduler.scheduleOnce(12 seconds) {
    system.terminate()
  }

}
