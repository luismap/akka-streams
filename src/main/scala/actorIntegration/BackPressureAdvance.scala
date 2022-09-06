package actorIntegration

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Sink, Source}
import org.apache.log4j.Logger

import java.util.Date
import scala.concurrent.duration.DurationInt

object BackPressureAdvance extends App {

  implicit val system = ActorSystem("system")
  implicit val materializer = ActorMaterializer()
  val scheduler = system.scheduler
  val logger = Logger.getLogger(this.getClass.getName)
  implicit val execCtx = system.dispatcher

  case class PagerEvent(app: String, desc: String, date: Date, instances: Int = 1)

  case class Notification(email: String, pagerEvent: PagerEvent)

  val eventSource = Source(List(
    PagerEvent("tree fell", "high winds fell tree", new Date()),
    PagerEvent("high waves", "bad weather pumping winds", new Date()),
    PagerEvent("blackout", "electricity is out", new Date()),
    PagerEvent("tree fell", "high winds fell tree", new Date()),
    PagerEvent("high waves", "bad weather pumping winds", new Date()),
    PagerEvent("blackout", "electricity is out", new Date()),
    PagerEvent("tree fell", "high winds fell tree", new Date()),
    PagerEvent("high waves", "bad weather pumping winds", new Date()),
    PagerEvent("blackout", "electricity is out", new Date()),
    PagerEvent("tree fell", "high winds fell tree", new Date()),
  ))

  def notify(notification: Notification): Unit = {
    logger.info(s"[notice] ${notification.email} got ${notification.pagerEvent}")
  }

  val notifier = Flow[PagerEvent].map(Notification("luis@home.com", _))

  scheduler.scheduleOnce(0 millis) {
    eventSource
      .via(notifier)
      .to(Sink.foreach[Notification](notify)).run()
  }

  /**
   * timer base source, cannot be back-pressure
   * - we can use conflate to aggregate events(works similar to fold)
   */
  def notifySlow(notification: Notification): Unit = {
    Thread.sleep(500)
    logger.info(s"[notice slow] ${notification.email} got ${notification.pagerEvent}")
  }

  val aggregateNotificationFlow = Flow[PagerEvent].conflate(
    (e1, e2) => {
      val total = e1.instances + e2.instances
      PagerEvent("aggregating", s"You have ${total} instances incoming", new Date(), total)
    }
  ).map(Notification("ann@home.com", _))

  scheduler.scheduleOnce(3 seconds) {
    logger.info("")
    eventSource
      .via(aggregateNotificationFlow)
      .async //running flow and sink in different actors to trigger backpressure
      .to(Sink.foreach[Notification](notifySlow))
      .run()

  }

  /**
   * Slow producers,
   * - you can artificially create data by extrapolating/expanding
   */

  val slowSource = Source(1 to 3).throttle(1, 500 millis)
  val extrapolatorFlow = Flow[Int].extrapolate(element => Iterator.from(element))
  scheduler.scheduleOnce(6 seconds) {
    slowSource
      .via(extrapolatorFlow)
      .to(Sink.foreach[Int](e => logger.info(s"[extrapolating] $e")))
      .run()
  }

  scheduler.scheduleOnce(12 seconds) {
    system.terminate()
  }

}
