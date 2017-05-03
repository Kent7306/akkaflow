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
import com.kent.pub.ShareData
import com.kent.db.LogRecorder
import com.kent.workflow.WorkflowActor.Start
import com.kent.main.Master.ShutdownCluster

class Worker extends ClusterRole {
  val i = 0
  init()
  import com.kent.main.Worker._
  def receive: Actor.Receive = {
    case MemberUp(member) => register(member, getMasterPath)
    case UnreachableMember(member) =>
      log.info("Member detected as Unreachable: {}", member)
    case MemberRemoved(member, previousStatus) =>
      log.info("Member is Removed: {} after {}", member.address, previousStatus)
    case state: CurrentClusterState =>
    case CreateAction(ani) => sender ! createActionActor(ani)
    case ShutdownCluster() => Worker.curActorSystems.foreach { _.terminate() }
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
  
  def init(){
    import com.kent.pub.ShareData._
     //日志记录器配置
     val logRecordConfig = (config.getString("workflow.log-mysql.user"),
                      config.getString("workflow.log-mysql.password"),
                      config.getString("workflow.log-mysql.jdbc-url"),
                      config.getBoolean("workflow.log-mysql.is-enabled")
                    )
      //创建日志记录器
      ShareData.logRecorder = context.actorOf(Props(LogRecorder(logRecordConfig._3,logRecordConfig._1,logRecordConfig._2,logRecordConfig._4)),"log-recorder")
  }
}

object Worker extends App {
  case class CreateAction(ani: ActionNodeInstance)
  import scala.collection.JavaConverters._
  val defaultConf = ConfigFactory.load()
  val workersConf = defaultConf.getStringList("workflow.nodes.workers").asScala.map { x => val y = x.split(":");(y(0),y(1)) }.toList
  var curActorSystems:List[ActorSystem] = List()
  
  workersConf.foreach {
    info =>
    val hostConf = "akka.remote.netty.tcp.hostname=" + info._1
    val portConf = "akka.remote.netty.tcp.port=" + info._2
      val config = ConfigFactory.parseString(hostConf)
        .withFallback(ConfigFactory.parseString(portConf))
        .withFallback(ConfigFactory.parseString("akka.cluster.roles = [worker]"))
        .withFallback(defaultConf)
      ShareData.config = config
      val system = ActorSystem("akkaflow", config)
      Worker.curActorSystems = Worker.curActorSystems :+ system
      val worker = system.actorOf(Worker.props, name = "worker")
  }
  
  def props = Props[Worker]
  
}