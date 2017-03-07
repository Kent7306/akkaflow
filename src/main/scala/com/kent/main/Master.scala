package com.kent.main

import akka.actor.Actor
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.ClusterEvent.UnreachableMember
import akka.cluster.ClusterEvent.MemberRemoved
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.ClusterEvent.MemberEvent
import com.kent.main.ClusterRole.Registration
import akka.actor.Terminated
import scala.concurrent.duration._
import akka.actor.ActorRef
import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import akka.actor.Props
import com.kent.coordinate.CoordinatorManager
import com.kent.workflow.WorkFlowManager
import com.kent.db.PersistManager
import com.kent.coordinate.CoordinatorManager.GetManagers
import com.kent.workflow.WorkFlowManager._
import com.kent.coordinate.CoordinatorManager.AddCoor
import com.kent.coordinate.CoordinatorManager.Start
import com.kent.main.Worker.CreateAction
import com.kent.workflow.node.ActionNodeInstance
import scala.concurrent.ExecutionContext.Implicits.global
import akka.pattern.ask
import akka.pattern.pipe
import akka.util.Timeout
import com.kent.main.Master.GetWorker
import com.kent.main.Master.AskWorker
import scala.util.Random
import com.typesafe.config.Config
import com.kent.pub.ShareData
import com.kent.mail.EmailSender
import com.kent.mail.EmailSender.EmailMessage
import com.kent.db.LogRecorder._
import com.kent.db.LogRecorder
import akka.cluster.Member
import akka.actor.ActorPath
import akka.actor.RootActorPath

class Master extends ClusterRole {
  var coordinatorManager: ActorRef = _
  var workflowManager: ActorRef = _
  
  def receive: Actor.Receive = { 
    case MemberUp(member) => 
      register(member, getHttpServerPath)
      log.info("Member is Up: {}", member.address)
    case UnreachableMember(member) =>
      log.info("Member detected as Unreachable: {}", member)
    case MemberRemoved(member, previousStatus) =>
      log.info("Member is Removed: {} after {}", member.address, previousStatus)
    case state: CurrentClusterState =>
    
    case _:MemberEvent => // ignore 
      
    case Registration() => {
      //worker请求注册
      context watch sender
      roler = roler :+ sender
      log.info("Worker registered: " + sender)
      log.info("Registered workers number: " + roler.size)
    }
    case Start() => this.start()
    case Terminated(workerActorRef) =>
      //worker终止，更新缓存的ActorRef
      roler = roler.filterNot(_ == workerActorRef)
    case AddWorkFlow(wfStr) => workflowManager ! AddWorkFlow(wfStr)
    case RemoveWorkFlow(wfId) => workflowManager ! RemoveWorkFlow(wfId)
    case AddCoor(coorStr) => coordinatorManager ! AddCoor(coorStr)
    case AskWorker(host: String) => sender ! GetWorker(askWorker(host: String))
    case ReRunWorkflowInstance(id: String) => workflowManager ! ReRunWorkflowInstance(id)
  }
  /**
   * 请求得到新的worker，动态分配
   */
  def askWorker(host: String):ActorRef = {
    //host为-1情况下，随机分配
    if(host == "-1") {
      roler(Random.nextInt(roler.size))
    }else{ //指定host分配
    	val list = roler.map { x => x.path.address.host.get }.toList
    	if(list.size > 0){
    		roler(Random.nextInt(list.size))       	  
    	}else{
    	  null
    	}
    }
  }
  
    /**
   * 获取http-server的路径
   */
  def getHttpServerPath(member: Member):Option[ActorPath] = {
    if(member.hasRole("http-server")){
    	Some(RootActorPath(member.address) /"user" / "http-server")    
    }else{
      None
    }
  }
  /**
   * 启动入口
   */
  def start():Boolean = {
    import com.kent.pub.ShareData._
    //mysql持久化参数配置
    val mysqlConfig = (config.getString("workflow.mysql.user"),
                      config.getString("workflow.mysql.password"),
                      config.getString("workflow.mysql.jdbc-url"),
                      config.getBoolean("workflow.mysql.is-enabled")
                    )
   //日志记录器配置
   val logRecordConfig = (config.getString("workflow.log-mysql.user"),
                      config.getString("workflow.log-mysql.password"),
                      config.getString("workflow.log-mysql.jdbc-url"),
                      config.getBoolean("workflow.log-mysql.is-enabled")
                    )
   //Email参数配置
   val emailConfig = (config.getString("workflow.email.hostname"),
                      config.getInt(("workflow.email.smtp-port")),
                      config.getString("workflow.email.account"),
                      config.getString("workflow.email.password"),
                      config.getBoolean("workflow.email.is-enabled")
                    )
                    
    //创建持久化管理器
    ShareData.persistManager = context.actorOf(Props(PersistManager(mysqlConfig._3,mysqlConfig._1,mysqlConfig._2,mysqlConfig._4)),"pm")
    //创建邮件发送器
    ShareData.emailSender = context.actorOf(Props(EmailSender(emailConfig._1,emailConfig._2,emailConfig._3,emailConfig._4,emailConfig._5)),"mail-sender")
    //创建日志记录器
    ShareData.logRecorder = context.actorOf(Props(LogRecorder(logRecordConfig._3,logRecordConfig._1,logRecordConfig._2,logRecordConfig._4)),"log-recorder")
    
    Thread.sleep(1000)
    //创建coordinator管理器
    coordinatorManager = context.actorOf(Props(CoordinatorManager(List())),"cm")
    //创建workflow管理器
    workflowManager = context.actorOf(Props(WorkFlowManager(List())),"wfm")
    coordinatorManager ! GetManagers(workflowManager,coordinatorManager)
    workflowManager ! GetManagers(workflowManager,coordinatorManager)
    Thread.sleep(1000)
    coordinatorManager ! Start()
    true
  }
}
object Master extends App {
  case class GetWorker(worker: ActorRef)
  case class AskWorker(host: String)
  def props = Props[Master]
  
  val defaultConf = ConfigFactory.load()
  val masterConf = defaultConf.getStringList("workflow.nodes.masters").get(0).split(":")
  val hostConf = "akka.remote.netty.tcp.hostname=" + masterConf(0)
  val portConf = "akka.remote.netty.tcp.port=" + masterConf(1)
  
  // 创建一个Config对象
  val config = ConfigFactory.parseString(portConf)
      .withFallback(ConfigFactory.parseString(hostConf))
      .withFallback(ConfigFactory.parseString("akka.cluster.roles = [master]"))
      .withFallback(defaultConf)
  ShareData.config = config
  
  // 创建一个ActorSystem实例
  val system = ActorSystem("akkaflow", config)
  val master = system.actorOf(Master.props, name = "master")
  
  master ! Start()
}