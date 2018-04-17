package com.example

import java.time.ZonedDateTime

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.io.StdIn
import scala.util.{Failure, Success}

import akka.actor.ActorSystem
import akka.kafka.{ConsumerSettings, ProducerMessage, ProducerSettings, Subscriptions}
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.{ActorMaterializer, KillSwitches}
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import spray.json._

object Main extends App with MessageJsonProtocol {

  import Message._

  implicit val system: ActorSystem = ActorSystem("kafka-example")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executor: ExecutionContext = system.dispatcher

  val conf = ConfigFactory.load()
  val bootstrapServers = conf.getString("kafka.bootstrapServers")

  // プロデューサー設定
  val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
    .withBootstrapServers(bootstrapServers)

  // プロデューサーstream
  val (pKillSwitch, pDone) = Source
    .tick(1.seconds, 10.seconds, None)
    .viaMat(KillSwitches.single)(Keep.right)
    .map { _ =>
      val msg = SendMessage("Hello", ZonedDateTime.now().toEpochSecond)
      ProducerMessage.Message(new ProducerRecord[String, String]("test.B", msg.toJson.compactPrint),
                              None)
    }
    .via(Producer.flow(producerSettings))
    .map { result =>
      println(s"success send. message: ${result.message.record.value()}")
      result
    }
    .toMat(Sink.ignore)(Keep.both)
    .run()

  pDone.onComplete {
    case Success(_) =>
      println("done producer.")
    case Failure(ex) =>
      println(s"fail send. reason: ${ex.getMessage}")
  }

  // コンシューマー設定
  val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers(bootstrapServers)
    .withGroupId("test")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  // コンシューマーstream
  val (cKillSwitch, cDone) = Consumer
    .committableSource(consumerSettings, Subscriptions.topics("test.B"))
    .viaMat(KillSwitches.single)(Keep.right)
    .map { consumed =>
      val msg = consumed.record.value().parseJson.convertTo[ConsumedMessage]
      println(s"success consume. message: ${msg.message}, timestamp: ${msg.timestamp}")
      consumed
    }
    .mapAsync(1) { msg =>
      msg.committableOffset.commitScaladsl()
    }
    .toMat(Sink.ignore)(Keep.both)
    .run()

  cDone.onComplete {
    case Success(_) =>
      println("done consumer.")
    case Failure(ex) =>
      println(s"fail consume. reason: ${ex.getMessage}")
  }

  println("start")

  StdIn.readLine()

  pKillSwitch.shutdown()
  cKillSwitch.shutdown()
  system.terminate()

  println("end")
}
