package intro

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import org.apache.log4j.Logger

import scala.concurrent.duration.DurationInt
import scala.util.Random

object IntroAkkaStreams extends App {

  implicit val system = ActorSystem("system")
  implicit val materializer = ActorMaterializer()
  val scheduler = system.scheduler
  implicit val execCtx = system.dispatcher

  val logger = Logger.getLogger(this.getClass.getName)

  def inc(x: Int) = x + 1

  def log(msg: Any): Unit = logger.info(s"[logging-msg] ${msg.toString}")

  //you can compose different types of graph
  //source-> processors/flows -> sinks
  val generator = 1 to 10
  val source = Source(generator)
  val flow = Flow[Int].map(inc)
  val sink = Sink.foreach(log(_))

  //source-> sink
  source.to(sink).run()

  //source-> process -> sink
  scheduler.scheduleOnce(2 seconds) {
    source.via(flow).to(sink).run()
  }

  //source cannot process null, use option (as per reactive streams manifest)
  scheduler.scheduleOnce(4 seconds) {
    try {
      val illegalSource = Source.single[String](null)
      illegalSource.to(sink).run()
    } catch {
      case e => logger.info(e.getMessage)
    }
  }

  scheduler.scheduleOnce(5 seconds) {
    //syntactic sugar
    //following is same as
    //Source(0 to 10).via(Flow[Int].map(_+1)
    val mapSource = Source(55 to 65).map( _ + 1)
    mapSource.to(sink).run()
  }

  //ex. create stream of names, pick he first two names with lenght > 5 chars
  val names = Array("jul", "ann","seb", "rosa", "soni", "sebastian", "manuel")
  val rnd = Random
  val generateNames = for (e <- Stream.from(0)) yield names(rnd.nextInt(names.length))
  val namesSource = Source(generateNames)

  scheduler.scheduleOnce(6 seconds) {
    namesSource
      .filter(_.length > 5)
      .take(2)
      .to(sink).run()
  }


  scheduler.scheduleOnce(12 seconds) {
    system.terminate()
  }


}
