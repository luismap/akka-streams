package actorIntegration

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout
import org.apache.log4j.Logger

import java.util.Date
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.Random


/**
 * usecases
 * - futures evaluated in parallel
 * - want order vs unordered handling
 * - lagging future stalls entire stream
 *
 * note: run futures in its own thread, to not stall default
 */
object StreamWithFutures extends App {

  implicit val system = ActorSystem("system")
  implicit val mat = ActorMaterializer()
  val scheduler = system.scheduler
  implicit val execCtx = system.dispatcher
  val logger = Logger.getLogger(this.getClass.getName)
  def log = (data: Any, info: String) => logger.info(s"[$info] $data")

  case class PagerEvent(app: String, desc: String, date: Date)

  val eventSource = Source(List(
    PagerEvent("tree fell", "high winds fell tree", new Date()),
    PagerEvent("high waves","bad weather pumping winds", new Date()),
    PagerEvent("blackout", "electricity is out", new Date()),
    PagerEvent("tree fell", "high winds fell tree", new Date()),
    PagerEvent("high waves","bad weather pumping winds", new Date()),
    PagerEvent("blackout", "electricity is out", new Date()),
    PagerEvent("tree fell", "high winds fell tree", new Date()),
    PagerEvent("high waves","bad weather pumping winds", new Date()),
    PagerEvent("blackout", "electricity is out", new Date()),
    PagerEvent("tree fell", "high winds fell tree", new Date()),
  ))

  object PagerService {
    private val engineers = Array("luis", "ann", "seb")
    private val emails = Map(
      "luis" -> "luis@google.com",
      "ann" -> "ann@google.com",
      "seb" -> "seb@google.com"
    )


    def processPagerEvent(pagerEvent: PagerEvent): Future[String] = Future {
      val engIdx = Random.nextInt(engineers.length)
      val email = emails(engineers(engIdx.toInt))
      Thread.sleep(100 )
      email
    }(system.dispatchers.lookup("with-futures-dispatcher"))
  }

  scheduler.scheduleOnce(0 millis) {
    val treeFellEvents = eventSource
    //map async guarantees order of elements
    val engineersNotify = treeFellEvents.mapAsync[String](4)(event => PagerService.processPagerEvent(event))
    engineersNotify
      .to(Sink.foreach(log(_,"engineer notified")))
      .run()

    (0 to 10 ).foreach(e => {Thread.sleep(100); log(e, "running in default-dispatcher")})
  }

  //a pager actor
  class PagerActor extends Actor with ActorLogging {
    implicit val execCtx = context.system.dispatchers.lookup("with-futures-dispatcher")
    private val engineers = Array("luis", "ann", "seb")
    private val emails = Map(
      "luis" -> "luis@google.com",
      "ann" -> "ann@google.com",
      "seb" -> "seb@google.com"
    )


    private def processPagerEvent(pagerEvent: PagerEvent): String = {
      val engIdx = Random.nextInt(engineers.length)
      val email = emails(engineers(engIdx.toInt))
      Thread.sleep(500 )
      email
    }

    override def receive: Receive = {
      case pagerEvent: PagerEvent =>
        log.info(s"[processing] $pagerEvent")
        Thread.sleep(100)
        sender() ! processPagerEvent(pagerEvent)
    }
  }

  scheduler.scheduleOnce(4 seconds) {
    import akka.pattern.ask
    val pagerActor = system.actorOf(Props[PagerActor],"pagerActor")
    implicit val timeout = Timeout(2 seconds)
    //because, mapAsync expects a future, good to combine with ask pattern
    val notifyEngineers = eventSource.mapAsyncUnordered[String](3 )(event => (pagerActor ? event ).mapTo[String])
    notifyEngineers.to(Sink.foreach(log(_,"notified using ask"))).run()
  }

  scheduler.scheduleOnce(12 second) {
    system.terminate()
  }


}
