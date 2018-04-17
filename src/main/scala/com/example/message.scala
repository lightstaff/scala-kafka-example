package com.example

import spray.json.{DefaultJsonProtocol, RootJsonFormat}

object Message {

  // 送信メッセージ
  final case class SendMessage(message: String, timestamp: Long)

  // 受信メッセージ
  final case class ConsumedMessage(message: String, timestamp: Long)

}

// メッセージ <-> json
trait MessageJsonProtocol extends DefaultJsonProtocol {

  import Message._

  implicit val sendMessageFormat: RootJsonFormat[SendMessage] = jsonFormat2(SendMessage)
  implicit val consumedMessageFormat: RootJsonFormat[ConsumedMessage] = jsonFormat2(ConsumedMessage)

}
