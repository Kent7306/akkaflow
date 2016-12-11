package com.kent.main

import akka.actor.Actor
import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import akka.actor.Props
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.ClusterEvent.UnreachableMember
import akka.cluster.ClusterEvent.MemberRemoved
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.ClusterEvent.MemberEvent
import akka.cluster.Member
import akka.actor.RootActorPath
import akka.actor.ActorPath
import akka.actor.ActorRef
import com.kent.workflow.node.ActionNodeInstance
import com.kent.workflow.ActionActor

class Worker extends ClusterRole {
  val i = 0
  import com.kent.main.Worker._
  def receive: Actor.Receive = {
    case MemberUp(member) => register(member, getMasterPath)
    case UnreachableMember(member) =>
      log.info("Member detected as Unreachable: {}", member)
    case MemberRemoved(member, previousStatus) =>
      log.info("Member is Removed: {} after {}", member.address, previousStatus)
    case state: CurrentClusterState =>
    
    case CreateAction(ani) => sender ! createActionActor(ani)
    case _:MemberEvent => // ignore 
  }
  /**
   * 创建action actor
   */
  def createActionActor(actionNodeInstance: ActionNodeInstance):ActorRef = {
		val actionActorRef = context.actorOf(Props(ActionActor(actionNodeInstance)) , 
		    actionNodeInstance.name)
		actionActorRef
  }
  /**
   * 获取master的路径
   */
  def getMasterPath(member: Member):Option[ActorPath] = {
    if(member.hasRole("master")){
    	Some(RootActorPath(member.address) /"user" / "master")    
    }else{
      None
    }
  }
}

object Worker extends App {
  case class CreateAction(ani: ActionNodeInstance)
  
  Seq("2851","2852").foreach {
    port =>
      val config = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + port)
        .withFallback(ConfigFactory.parseString("akka.cluster.roles = [worker]"))
        .withFallback(ConfigFactory.load())
      
      val system = ActorSystem("workflow-system", config)
      val processingActor = system.actorOf(Worker.props, name = "worker")
      system.log.info("Processing Actor: " + processingActor)
  }
  
  def props = Props[Worker]
  
}