package com.kent.main

import akka.actor.Actor
import akka.actor.ActorLogging

class Master extends Actor with ActorLogging {
  def receive: Actor.Receive = {
    ???
  }
}

object Master {
  case class Request(status: String, msg: String, data: Any)
  case class Response(status: String, msg: String, data: Any)
}