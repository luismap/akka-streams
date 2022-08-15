package graphs

import akka.NotUsed
import akka.actor.{Actor, ActorSystem}
import akka.stream.{ActorMaterializer, ClosedShape, FanOutShape, FanOutShape2, UniformFanInShape}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, RunnableGraph, Sink, Source, Zip, ZipWith}
import org.apache.log4j.Logger

import scala.concurrent.duration.DurationInt

object AdvOpenGraph extends App {

  implicit val system = ActorSystem("system")
  implicit val materializer = ActorMaterializer()
  val scheduler = system.scheduler
  implicit val exeCtx = system.dispatcher
  val logger = Logger.getLogger(this.getClass.getName)
  val log = (e: Any, info: String) => logger.info(s"[$info] $e")


  /**
   * a graph that takes 3 inputs and produce the max item
   * -> in1
   * ******max     -> in2
   * -> in1
   * *********max      -> out
   * ************  ->in2
   *
   * this graph is static(i.e we created a component)
   */

  val maxOfThree = GraphDSL.create() {
    implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._
      val inputOne = builder.add(ZipWith[Int, Int, Int]((a, b) => Math.max(a, b)))
      val inputTwo = builder.add(ZipWith[Int, Int, Int]((a, b) => Math.max(a, b)))

      inputOne.out ~> inputTwo.in0

      UniformFanInShape(inputTwo.out, inputOne.in0, inputOne.in1, inputTwo.in1)

  }

  import scala.util.{Random => rnd}

  def yielder = for {_ <- 0 to 20} yield rnd.nextInt(20)

  scheduler.scheduleOnce(0 millis) {

    val s1 = Source(yielder)
    val s2 = Source(yielder)
    val s3 = Source(yielder)
    val sink = Sink.foreach(log(_, "max-of-three"))

    //creating the graph
    val runnableMaxOfThree = RunnableGraph.fromGraph(
      GraphDSL.create() {
        implicit builder: GraphDSL.Builder[NotUsed] =>
          import GraphDSL.Implicits._
          val zipS = builder.add(ZipWith[Int, Int, Int, (Int, Int, Int)]((a, b, c) => (a, b, c)))
          val broadcastOne = builder.add(Broadcast[Int](2))
          val broadcastTwo = builder.add(Broadcast[Int](2))
          val broadcastThree = builder.add(Broadcast[Int](2))

          s1 ~> broadcastOne
          s2 ~> broadcastTwo
          s3 ~> broadcastThree

          broadcastOne.out(0) ~> zipS.in0
          broadcastTwo.out(0) ~> zipS.in1
          broadcastThree.out(0) ~> zipS.in2


          val maxOfThreeIns = builder.add(maxOfThree)
          val zip = builder.add(Zip[Int, (Int, Int, Int)])
          broadcastOne.out(1) ~> maxOfThreeIns.in(0)
          broadcastTwo.out(1) ~> maxOfThreeIns.in(1)
          broadcastThree.out(1) ~> maxOfThreeIns.in(2)

          maxOfThreeIns.out ~> zip.in0
          zipS.out ~> zip.in1
          zip.out ~> sink

          ClosedShape
      }
    )

    runnableMaxOfThree.run()
  }

  //fan out component
  /**
   * flag transactions
   * #tranx -> broadcast -> filter -> extract id
   * #                   -> print
   */

  scheduler.scheduleOnce(3 seconds) {
    case class Transaction(name: String, amount: Int)
    val names = Array("luis", "anna", "sebastian")

    def tranx = Transaction(names(rnd.nextInt(3)), 95 + rnd.nextInt(10))

    val source = Source(for (_ <- 0 to 20) yield tranx)
    val logTranx = Sink.foreach(log(_, "transaction"))
    val logSuspicious = Sink.foreach(log(_, "suspicious"))

    val transactionComponent = GraphDSL.create() {
      implicit builder: GraphDSL.Builder[NotUsed] =>
        import GraphDSL.Implicits._
        val broadcast = builder.add(Broadcast[Transaction](2))
        val filter = builder.add(Flow[Transaction].filter(_.amount > 100))
        val extractor = builder.add(Flow[Transaction].map(a => s"${a.name} suspected of fraud amout ${a.amount}"))

        broadcast.out(0) ~> filter ~> extractor

        new FanOutShape2(broadcast.in, broadcast.out(1), extractor.out)
    }

    val graph = RunnableGraph.fromGraph(
      GraphDSL.create() {
        implicit builder: GraphDSL.Builder[NotUsed] =>
          import GraphDSL.Implicits._

          //declare components
          val tComponent = builder.add(transactionComponent)

          //connect
          source ~> tComponent.in
          tComponent.out0 ~> logTranx
          tComponent.out1 ~> logSuspicious

          ClosedShape
      }
    )

    graph.run()

  }


  scheduler.scheduleOnce(12 seconds) {
    system.terminate()
  }

}
