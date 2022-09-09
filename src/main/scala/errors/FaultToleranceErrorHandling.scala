import akka.actor.ActorSystem
import akka.stream.Supervision.{Resume, Stop}
import akka.stream.{ActorAttributes, ActorMaterializer}
import akka.stream.scaladsl.RestartSource
import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger

import scala.concurrent.duration.DurationInt
import scala.util.Random

object FaultToleranceErrorHandling extends App {

  implicit val system = ActorSystem("system", config = ConfigFactory.load().getConfig("faultTolerance"))
  implicit val materializer = ActorMaterializer()
  val scheduler = system.scheduler
  implicit val execCtx = system.dispatcher
  val logger = Logger.getLogger(this.getClass.getName)

  /**
   * logging in streams
   * - get the logger from component
   * - source component logs a DEBUG level by default
   * - will log elements passing through
   * - to see the logs, the stream guardian need to be logging at debug
   */

  val source = Source(1 to 100).map(e => if (e % 9 == 0) throw new RuntimeException else e)

  //logging
  scheduler.scheduleOnce(0 millis) {
    try {
      source
        .log("log numbers")
        .to(Sink.ignore)
        .run()
    } catch {
      case _: RuntimeException => logger.info("caught exception")
    }
  }

  scheduler.scheduleOnce(2 seconds) {
    //handling exception inside the stream
    source
      .recover { case _: RuntimeException => Int.MinValue } //handling exceptions
      .log("gracefully shutdown")
      .to(Sink.ignore)
      .run()

  }

  scheduler.scheduleOnce(4 seconds) {
    //recovering with another stream
    source
      .recoverWithRetries(3, {
        case _: RuntimeException => Source(12 to 20)
      })
      .log("recoverWith")
      .to(Sink.ignore)
      .run()
  }
  //backoff supervision
  scheduler.scheduleOnce(6 seconds) {
    val sourceBackoff = RestartSource.onFailuresWithBackoff(
      minBackoff = 1 seconds,
      maxBackoff = 30 seconds,
      randomFactor = 0.2)(
      () => {
        val random = Random.nextInt(20) //simulating failures in stream
        Source(1 to 12).map(
          e => if (e == random) throw new RuntimeException else e
        )
      }
    )

    sourceBackoff
      .log("sourceBackoff")
      .to(Sink.ignore)
      .run()
  }

  //supervision
  scheduler.scheduleOnce(12 seconds) {
    val s1 = Source(1 to 20).map(e => if (e == 13) throw new RuntimeException else e)
    val sourceWithSupervision =
      s1.withAttributes(ActorAttributes.supervisionStrategy {
        case _: RuntimeException => Resume
        case _ => Stop
      })

    sourceWithSupervision
      .log("source with supervision")
      .to(Sink.ignore)
      .run()
  }


  scheduler.scheduleOnce(22 seconds) {
    system.terminate()
  }
}