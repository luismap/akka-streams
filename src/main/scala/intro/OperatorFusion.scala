package intro

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import org.apache.log4j.Logger

import scala.concurrent.duration.DurationInt

object OperatorFusion extends App {

  implicit val system = ActorSystem("ops-fusion")
  implicit val mat = ActorMaterializer()
  implicit val execCtx = system.dispatcher
  val scheduler = system.scheduler
  val log = Logger.getLogger(this.getClass.getName)

  def log(s: Any): Unit = log.info(s"[logging] ${s.toString}")

  val source = Source(1 to 10)
  val inc = Flow[Int].map(_ + 1)
  val filter = Flow[Int].filter(_ % 2 == 0)
  val sink = Sink.foreach((a: Int) => log(a))

  /**
   * the entire graphs runs under same actor
   * - the processing is seq/runs in same thread
   * - this is call operator/component fusion
   * - akka streams does it by default
   *
   */
  source.via(inc).via(filter).to(sink).run()

  /**
   * operator fusion introduces a problem when processing is
   * expensive
   */

  val heavyInc = Flow[Int].map((x: Int) => {
    Thread.sleep(500)
    x + 1
  })
  val heavyFilter = Flow[Int].filter((x: Int) => {
    Thread.sleep(200)
    x % 2 == 0
  })

  //heavy graph with operator fusion

  scheduler.scheduleOnce(1 seconds) {
    source.via(heavyInc).via(heavyFilter).to(sink).run()
  }

  /**
   * to solve the operator fusion problem, use async boundaries
   */
  scheduler.scheduleOnce(7 seconds) {
    source.via(heavyInc).async
      .via(heavyFilter).async
      .to(sink).run()
  }

  /**
   * ordering guarantees
   * - operator fusion garantees order in entire flow
   * - async boundaries guarantees order in same level operator/component i.e
   * (elements at same level will always appear in order)
   */

  scheduler.scheduleOnce(12 seconds) {
    Source(1 to 3)
      .map((x: Int) => {log(s"flow A value: $x"); x})
      .map((x: Int) => {log(s"flow B value: $x"); x})
      .map((x: Int) => {log(s"flow C value: $x"); x})
      .to(Sink.ignore)
      .run()
  }

  scheduler.scheduleOnce(14 seconds) {
    Source(1 to 3)
      .map((x: Int) => {log(s"flow A value: $x"); x}).async
      .map((x: Int) => {log(s"flow B value: $x"); x}).async
      .map((x: Int) => {log(s"flow C value: $x"); x}).async
      .to(Sink.ignore)
      .run()
  }



  scheduler.scheduleOnce(24 seconds) {
    system.terminate()
  }

}
