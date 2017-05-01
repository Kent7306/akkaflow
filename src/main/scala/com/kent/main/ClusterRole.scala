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
  val cluster = Cluster(context.system) 
  // 用来缓存下游注册过来的roler
  var roler = IndexedSeq.empty[ActorRef] 
  
  override def preStart(): Unit = {
    // 订阅集群事件
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberUp], classOf[UnreachableMember], classOf[MemberEvent])
  }
  
   /**
   * 下游子系统节点发送注册消息
   */
  def register(member: Member, createPath: (Member) => Option[ActorPath]): Unit = { 
    val pathOpt = createPath(member)
    if(!pathOpt.isEmpty){
    	val actorSelection = context.actorSelection(pathOpt.get)
    	actorSelection ! Registration()      
    }
  }
   /**
   * 下游子系统节点发送解除注册消息
   */
  def unRegister(member: Member, createPath: (Member) => Option[ActorPath]): Unit = { 
    val pathOpt = createPath(member)
    if(!pathOpt.isEmpty){
    	val actorSelection = context.actorSelection(pathOpt.get)
    	actorSelection ! UnRegistration()      
    }
  }
}

object ClusterRole {
  case class Request(status: String, msg: String, data: Any) extends Serializable
  case class Response(status: String, msg: String, data: Any) extends Serializable
  case class Registration() extends Serializable
  case class UnRegistration() extends Serializable
}