package com.kent.pub

import com.typesafe.config.Config
import akka.actor.ActorRef

/**
 * 数据共享单例
 */
object ShareData {
  var config: Config = _
  var persistManager:ActorRef = _
  var emailSender:ActorRef = _
}