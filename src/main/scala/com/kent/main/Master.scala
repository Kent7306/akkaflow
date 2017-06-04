package com.kent.main

import akka.actor.Actor
import akka.cluster.ClusterEvent._
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
import akka.pattern.{ ask, pipe }
import com.kent.workflow.node.ActionNodeInstance
import scala.concurrent.ExecutionContext.Implicits.global
import akka.util.Timeout
import com.kent.pub.Event._
import scala.util.Random
import com.typesafe.config.Config
import com.kent.mail.EmailSender
import com.kent.db.LogRecorder
import akka.cluster.Member
import akka.actor.ActorPath
import akka.actor.RootActorPath
import com.kent.db.XmlLoader
import scala.util.Success
import scala.concurrent.Future


class Master extends ClusterRole {
  var coordinatorManager: ActorRef = _
  var workflowManager: ActorRef = _
  var xmlLoader: ActorRef = _
  var httpServerRef:ActorRef = _
  var isStarted = false
  implicit val timeout = Timeout(20 seconds)
  init()
  
  import akka.actor.OneForOneStrategy
  override def supervisorStrategy = OneForOneStrategy(){
    case _:Exception => import akka.actor.SupervisorStrategy._;Escalate
  }
  def init(){
    import com.kent.main.Master._
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
  //xmlLoader参数配置
  val xmlLoaderConfig = (config.getString("workflow.xml-loader.workflow-dir"),
                      config.getString(("workflow.xml-loader.coordinator-dir")),
                      config.getInt("workflow.xml-loader.scan-interval")
                    )
                    
    //创建持久化管理器
    Master.persistManager = context.actorOf(Props(PersistManager(mysqlConfig._3,mysqlConfig._1,mysqlConfig._2,mysqlConfig._4)),"pm")
    //创建邮件发送器
    Master.emailSender = context.actorOf(Props(EmailSender(emailConfig._1,emailConfig._2,emailConfig._3,emailConfig._4,emailConfig._5)),"mail-sender")
    //创建日志记录器
    Master.logRecorder = context.actorOf(Props(LogRecorder(logRecordConfig._3,logRecordConfig._1,logRecordConfig._2,logRecordConfig._4)),"log-recorder")
    //创建coordinator管理器
    coordinatorManager = context.actorOf(Props(CoordinatorManager(List())),"cm")
    //创建workflow管理器
    workflowManager = context.actorOf(Props(WorkFlowManager(List())),"wfm")
    //创建xml装载器
    xmlLoader = context.actorOf(Props(XmlLoader(xmlLoaderConfig._1,xmlLoaderConfig._2, xmlLoaderConfig._3)),"xml-loader")
    
    Thread.sleep(3000)
    log.info("初始化成功")
  }
  
  def receive: Actor.Receive = { 
    case MemberUp(member) => 
      register(member, getHttpServerPath)
      if(member.hasRole("http-server")){
        val path = RootActorPath(member.address) /"user" / "http-server"
        import scala.concurrent.duration._
        val result = context.actorSelection(path).resolveOne(20 second)
        result.map { this.httpServerRef = _ }
      }
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
      log.info("注册Worker: " + sender)
      log.info("当前注册的Worker数量: " + roler.size)
      if(isStarted) self ! Start()
    }
    case Start() => this.start()
    case Terminated(workerActorRef) =>
      //worker终止，更新缓存的ActorRef
      roler = roler.filterNot(_ == workerActorRef)
    case AddWorkFlow(wfStr) => workflowManager ! AddWorkFlow(wfStr)
    case RemoveWorkFlow(wfId) => workflowManager ! RemoveWorkFlow(wfId)
    case AddCoor(coorStr) => coordinatorManager ! AddCoor(coorStr)
    case AskWorker(host: String) => sender ! GetWorker(allocateWorker(host: String))
    case ReRunWorkflowInstance(id: String) => workflowManager ! ReRunWorkflowInstance(id)
    case ShutdownCluster() =>  shutdownCluster(sender)
    case CollectClusterInfo() => collectClusterInfo(sender)
                               
  }
  /**
   * 请求得到新的worker，动态分配
   */
  private def allocateWorker(host: String):ActorRef = {
    //host为-1情况下，随机分配
    if(host == "-1") {
      roler(Random.nextInt(roler.size))
    }else{ //指定host分配
    	val list = roler.map { _.path.address.host.get }.toList
    	if(list.size > 0)
    		roler(Random.nextInt(list.size))       	  
    	else
    	  null
    }
  }
  
    /**
   * 获取http-server的路径
   */
  private def getHttpServerPath(member: Member):Option[ActorPath] = {
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
    this.isStarted = true
    coordinatorManager ! GetManagers(workflowManager,coordinatorManager)
    workflowManager ! GetManagers(workflowManager,coordinatorManager)
    xmlLoader ! Start()
    coordinatorManager ! Start()
    workflowManager ! Start()
    true
  }
  
  def collectClusterInfo(sdr: ActorRef) = {
    import com.kent.pub.Event.ActorType._
    var allActorInfo = new ActorInfo()
    allActorInfo.name = "top"
    allActorInfo.atype = ROLE
    
    val ai = new ActorInfo()
    
    ai.ip = context.system.settings.config.getString("akka.remote.netty.tcp.hostname")
    ai.port = context.system.settings.config.getInt("akka.remote.netty.tcp.port")
    ai.atype = ROLE
    ai.name = self.path.name + s"(${ai.ip}:${ai.port})"
    allActorInfo.subActors = allActorInfo.subActors :+ ai
    
    val es = new ActorInfo()
    es.name = Master.emailSender.path.name
    es.atype = DEAMO
    ai.subActors  = ai.subActors :+ es
    val pm = new ActorInfo()
    pm.name = Master.persistManager.path.name
    pm.atype = DEAMO
    ai.subActors  = ai.subActors :+ pm
    val lr = new ActorInfo()
    lr.name = Master.logRecorder.path.name
    lr.atype = DEAMO
    ai.subActors  = ai.subActors :+ lr
    val xl = new ActorInfo()
    xl.name = xmlLoader.path.name
    xl.atype = DEAMO
    ai.subActors  = ai.subActors :+ xl
    val cm = new ActorInfo()
    cm.name = coordinatorManager.path.name
    cm.atype = DEAMO
    ai.subActors  = ai.subActors :+ cm
    val wmResultF = (workflowManager ? CollectClusterInfo())
      .mapTo[GetClusterInfo].map { 
      case GetClusterInfo(x) => x 
    }
    val workerResultFs = this.roler.map { x => 
      (x ? CollectClusterInfo()).mapTo[GetClusterInfo].map{case GetClusterInfo(y) => y} 
    }.toList
    val resultLF = Future.sequence(workerResultFs)
    val resultF = for{
      x <- wmResultF
      y <- resultLF
    } yield (x, y)
    resultF.andThen{case Success(x) => 
      ai.subActors = ai.subActors :+ x._1
      allActorInfo.subActors = allActorInfo.subActors ++ x._2
      sdr ! ResponseData("success","成功获取集群信息", allActorInfo.getClusterInfo())
    }
    
  }
  
  def shutdownCluster(sdr: ActorRef) = {
     coordinatorManager ! Stop()
     xmlLoader ! Stop()
     val result = (workflowManager ? KllAllWorkFlow()).mapTo[ResponseData]
     result.andThen{
        case Success(x) => 
                  roler.foreach { _ ! ShutdownCluster() }
                   sdr ! ResponseData("success","worker角色与master角色已关闭",null)
                   Master.system.terminate()
      }
  }
}
object Master extends App {
  def props = Props[Master]
  var curSystem:ActorSystem = _
  var persistManager:ActorRef = _
  var emailSender: ActorRef = _
  var logRecorder: ActorRef = _
  
  val defaultConf = ConfigFactory.load()
  val masterConf = defaultConf.getStringList("workflow.nodes.masters").get(0).split(":")
  val hostConf = "akka.remote.netty.tcp.hostname=" + masterConf(0)
  val portConf = "akka.remote.netty.tcp.port=" + masterConf(1)
  
  // 创建一个Config对象
  val config = ConfigFactory.parseString(portConf)
      .withFallback(ConfigFactory.parseString(hostConf))
      .withFallback(ConfigFactory.parseString("akka.cluster.roles = [master]"))
      .withFallback(defaultConf)
  
  // 创建一个ActorSystem实例
  val system = ActorSystem("akkaflow", config)
  Master.curSystem = system
  val master = system.actorOf(Master.props, name = "master")
}