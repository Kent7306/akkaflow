package com.kent.pub

import com.typesafe.config.Config
import akka.actor.ActorRef
import akka.actor.ActorSystem

/**
 * 数据共享单例
 */
object ShareData {
  var config: Config = _
  var persistManager:ActorRef = _
  var emailSender:ActorRef = _
  var logRecorder:ActorRef = _
}