package com.kent.main

import akka.actor.ActorLogging
import akka.actor._

import scala.concurrent.duration._

class RemoteLookupProxy(path: String) extends Actor with ActorLogging {
  
  context.setReceiveTimeout(3 seconds)
  sendIdentifyRequest()
  
  def sendIdentifyRequest(): Unit = {
    val selection = context.actorSelection(path)
    selection ! Identify(path)
  }
  
  def receive = identify
  
	import com.kent.main.RemoteLookupProxy._
  def identify: Actor.Receive = {
    case ActorIdentity(`path`, Some(actor)) => 
      log.info("switch to active state")
      context.become(active(actor))
      context.watch(actor)
    case ActorIdentity(`path`, None) =>
      log.error(s"Remote actor with path $path is not available.")
    case ReceiveTimeout => 
      log.error(s"remote actor is time out.")
      sendIdentifyRequest()
    case GetStatus() =>
      sender ! Status("identify")
    case msg:Any => 
      log.error(s"Ignoring message $msg, remote actor is not ready yet.")
  }
  
  def active(actor: ActorRef):Receive = {
    case Terminated(actorRef) =>
      log.info(s"Actor $actorRef terminated.")
      log.info("switching to identify state.")
      context.become(identify)
      context.setReceiveTimeout(3 seconds)
      sendIdentifyRequest()
    case GetStatus() =>
      sender ! Status("active")
    case msg:Any => actor forward msg
      
  }
}

object RemoteLookupProxy {
  case class GetStatus()
  case class Status(str: String)
}