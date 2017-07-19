package com.kent.pub

import akka.actor.Actor
import akka.actor.ActorLogging
import com.kent.pub.Event._
import com.kent.pub.ClusterRole._
import com.kent.pub.ClusterRole.ActorType._
import akka.pattern.{ ask, pipe }
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import akka.util.Timeout
import scala.concurrent.Future
import scala.concurrent.Await

abstract class ActorTool extends Actor with ActorLogging{
  implicit val timeout = Timeout(20 seconds)
  val askTimeout = 20 seconds
  
  def indivivalReceive: Actor.Receive
  
  def receive: Actor.Receive = indivivalReceive  orElse commonReceice
  
  
  def commonReceice: Actor.Receive = {
    case CollectActorInfo() => sender ! collectActorInfo()
  }
  /**
   * 该actor包括子actor的信息
   */
  def collectActorInfo():ActorInfo = {
		val ai = new ActorInfo()
    if(this.isInstanceOf[ClusterRole]){
      ai.ip = context.system.settings.config.getString("akka.remote.netty.tcp.hostname")
      ai.port = context.system.settings.config.getInt("akka.remote.netty.tcp.port")
      ai.atype = ROLE
      ai.name = s"${self.path.name}(${ai.ip}:${ai.port})"
    }else{
      ai.ip = context.system.settings.config.getString("akka.remote.netty.tcp.hostname")
      ai.port = context.system.settings.config.getInt("akka.remote.netty.tcp.port")
      ai.atype = ROLE
      ai.name = s"${self.path.name}(${self.hashCode()})"
    }
    val caiFs = context.children.map { child =>
      (child ? CollectActorInfo()).mapTo[ActorInfo]
    }.toList
    val caisF = Future.sequence(caiFs)
    val cais = Await.result(caisF, askTimeout)
    ai.subActors = cais
    ai
  }
}