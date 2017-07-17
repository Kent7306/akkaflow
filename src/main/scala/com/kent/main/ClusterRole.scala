package com.kent.main

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.cluster.Cluster
import akka.actor.ActorRef
import akka.cluster.ClusterEvent.InitialStateAsEvents
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.ClusterEvent.UnreachableMember
import akka.cluster.ClusterEvent.MemberEvent
import akka.cluster.Member
import akka.actor.ActorPath
import com.kent.main.ClusterRole.Registration
import com.kent.main.ClusterRole.UnRegistration

abstract class ClusterRole extends Actor with ActorLogging {
  // 创建一个Cluster实例
  implicit val cluster = Cluster(context.system) 
  
  override def preStart(): Unit = {
    // 订阅集群事件
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberUp], classOf[UnreachableMember], classOf[MemberEvent])
  }
  
  def getHostPortKey():String = {
    val host = context.system.settings.config.getString("akka.remote.netty.tcp.hostname")
    val port = context.system.settings.config.getString("akka.remote.netty.tcp.port")
    s"${host}:${port}"
  }
}

object ClusterRole {
  case class Request(status: String, msg: String, data: Any) extends Serializable
  case class Response(status: String, msg: String, data: Any) extends Serializable
  case class Registration() extends Serializable
  case class UnRegistration() extends Serializable
}