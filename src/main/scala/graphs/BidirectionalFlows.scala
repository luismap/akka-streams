package graphs

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, BidiShape, ClosedShape}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, RunnableGraph, Sink, Source}
import org.apache.log4j.Logger

import scala.concurrent.duration.DurationInt

object BidirectionalFlows extends App {

  implicit val system = ActorSystem("system")
  implicit val materializer = ActorMaterializer()
  val scheduler = system.scheduler
  implicit val execCtx = system.dispatcher
  val logger = Logger.getLogger(this.getClass.getName)
  val log = (data: Any, info: String) => logger.info(s"[$info] $data")

  def encrypt(n: Int)(s: String) = s.map(c => (c + n).toChar)

  def decrypt(n: Int)(s: String) = s.map(c => (c - n).toChar)

  /**
   * biderectional flow
   * _ -> f -> _
   * _ <- f <- _
   * f(x,y)
   * ex.
   * s -> encrypt -> encryptedS
   * s <- decript <- encryptedS
   *
   */
  val biDirectionalEncryptorShape = GraphDSL.create() {
    implicit builder: GraphDSL.Builder[NotUsed] =>
      val iflowShape = builder.add(Flow[String].map(encrypt(3)))
      val oflowShape = builder.add(Flow[String].map(decrypt(3)))

      BidiShape.fromFlows(iflowShape, oflowShape)
  }
  val data = "I love akka. looking at how it works".split("\\s+").toList
  val source = Source(data)

  scheduler.scheduleOnce(0 millis) {
    val edGraph = RunnableGraph.fromGraph(
      GraphDSL.create() {
        implicit builder: GraphDSL.Builder[NotUsed] =>
          import GraphDSL.Implicits._
          val bidiShape = builder.add(biDirectionalEncryptorShape)
          val sourceShape = builder.add(source)
          val broadcastEn = builder.add(Broadcast[String](2))
          val sinkEn = builder.add(Sink.foreach(log(_,"encrypted")))
          val sinkDe = builder.add(Sink.foreach(log(_,"decrypted")))

          sourceShape.out ~> bidiShape.in1 ; bidiShape.out1 ~> broadcastEn ; broadcastEn.out(0) ~> sinkEn
          sinkDe <~ bidiShape.out2 ; bidiShape.in2 <~ broadcastEn.out(1)
          ClosedShape
      }
    )

    edGraph.run()

    /**
     *
     * biderectional flow use-case
     * - encryption/decryption
     * - encoding/decoding
     * - serializing/deserializing
     */
  }


  scheduler.scheduleOnce(12 seconds) {
    system.terminate()
  }
}
