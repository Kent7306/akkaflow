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
import com.kent.main.Master.Registration

class Master extends Actor with ActorLogging {
  // 创建一个Cluster实例
  val cluster = Cluster(context.system) 
  // 用来缓存下游注册过来的worker
  var workers = IndexedSeq.empty[ActorRef] 
  
  override def preStart(): Unit = {
    // 订阅集群事件
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberUp], classOf[UnreachableMember], classOf[MemberEvent])
  }
  
   /**
   * 下游子系统节点发送注册消息
   */
  def register(member: Member, createPath: (Member) => ActorPath): Unit = { 
    val actorPath = createPath(member)
    val actorSelection = context.actorSelection(actorPath)
    //log.info("Actor path: " + actorPath)
    //log.info(actorSelection.toString()+"____________________");
    actorSelection ! Registration()
  }
  
  def receive: Actor.Receive = {
    ???
  }
}

object Master {
  case class Request(status: String, msg: String, data: Any)
  case class Response(status: String, msg: String, data: Any)
  case class Registration()
}