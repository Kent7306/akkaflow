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
import com.kent.ddata.HaDataStorager
import com.kent.ddata.HaDataStorager.AddWorkflow
import com.kent.workflow.WorkflowInfo
import com.kent.ddata.HaDataStorager._
import com.kent.workflow.WorkflowInstance
import com.kent.coordinate.Coordinator
import com.kent.coordinate.CronComponent


class Master extends ClusterRole {
  var coordinatorManager: ActorRef = _
  var workflowManager: ActorRef = _
  var xmlLoader: ActorRef = _
  var httpServerRef:ActorRef = _
  //其他
  var otherMaster:ActorRef = _
  //是否已经启动了
  var isStarted = false
  //是否是活动节点
  var isActiveMember = true
  implicit val timeout = Timeout(20 seconds)
  /**
   * 监控策略  
   */
  import akka.actor.OneForOneStrategy
  override def supervisorStrategy = OneForOneStrategy(){
    case _:Exception => akka.actor.SupervisorStrategy.Escalate
  }
  init()
    
  /**
   * 初始化
   */
  def init(){
    //创建集群高可用数据分布式数据寄存器
    Master.haDataStorager = context.actorOf(Props[HaDataStorager],"ha-data")
  }
  
  def receive: Actor.Receive = { 
    case MemberUp(member) => 
      register(member, getHttpServerPath)
      //设置httpserver引用
      if(member.hasRole("http-server")){
        val path = getHttpServerPath(member).get
        val result = context.actorSelection(path).resolveOne(20 second)
        result.andThen { 
          case Success(x) => this.httpServerRef = x 
        }
      }
      //设置其他master引用
      if(member.hasRole("master")){
        val path = RootActorPath(member.address) /"user" / "master"
        	val result = context.actorSelection(path).resolveOne(20 second)
        			result.andThen {
        			case Success(x) => 
        			  if(x != self){
        			    println("增加master")
        				  this.otherMaster = x
        				  //备份主节点监控活动主节点
        				  if(!this.isActiveMember) {
        				    println("开始监控活动master")
        				    context.watch(x)
        				  }
        			  }
        	}          
      }

      log.info("Member is Up: {}", member.address)
    case UnreachableMember(member) =>
      println("member 不能到达："+member.address)
      //移除master引用
     /* if(member.hasRole("master")){
        this.otherMaster = null
      }*/
      log.info("Member detected as Unreachable: {}", member)
    case MemberRemoved(member, previousStatus) =>
      log.info("Member is Removed: {} after {}", member.address, previousStatus)
    case state: CurrentClusterState =>
    
    case _:MemberEvent => // ignore 
    //worker请求注册
    case Registration() =>
      context watch sender
      roler = roler :+ sender
      log.info("注册Worker: " + sender)
      log.info("当前注册的Worker数量: " + roler.size)
      if(!isStarted && isActiveMember) {
        startActors()
      }
    case StartIfActive(isAM) => if(isAM) active() else standby()
    //worker终止，更新缓存的ActorRef
    case Terminated(ar) => 
      //若是worker，则删除
      roler = roler.filterNot(_ == ar)
      println("down掉的：" + ar)
      println("监控的master:" + otherMaster)
      //若是活动主节点，则需要激活当前备份节点
      if(ar == otherMaster){
        println("开始切换到活动master")
        otherMaster = null
        this.active()
      }
      
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
    if(roler.size > 0) {
      //host为-1情况下，随机分配
      if(host == "-1") {
        roler(Random.nextInt(roler.size))
      }else{ //指定host分配
      	val list = roler.map { _.path.address.host.get }.toList
      	if(list.size > 0) roler(Random.nextInt(list.size)) else null
      }
    }else{
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
   * 作为活动主节点启动
   */
  def active():Future[Boolean] = {
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
    
    this.isActiveMember = true
    //把其他master设置为备份主节点
		if(this.otherMaster!= null){
		  this.otherMaster ! StartIfActive(false)
		}
    //获取distributed数据来构建子actors
    val ddataF = getDData()
    ddataF.map{ 
      case DistributeData(wfs,coors,rwfis, wwfis,xmlFiles) => 
        //创建持久化管理器
        Master.persistManager = context.actorOf(Props(PersistManager(mysqlConfig._3,mysqlConfig._1,mysqlConfig._2,mysqlConfig._4)),"pm")
        //创建邮件发送器
        Master.emailSender = context.actorOf(Props(EmailSender(emailConfig._1,emailConfig._2,emailConfig._3,emailConfig._4,emailConfig._5)),"mail-sender")
        //创建日志记录器
        Master.logRecorder = context.actorOf(Props(LogRecorder(logRecordConfig._3,logRecordConfig._1,logRecordConfig._2,logRecordConfig._4)),"log-recorder")
        //创建xml装载器
        xmlLoader = context.actorOf(Props(XmlLoader(xmlLoaderConfig._1,xmlLoaderConfig._2, xmlLoaderConfig._3, xmlFiles)),"xml-loader")
        //创建coordinator管理器
        coors.foreach { x => x.cron = CronComponent(x.cronStr,x.startDate,x.endDate) }
        coordinatorManager = context.actorOf(Props(CoordinatorManager(coors)),"cm")
        //创建workflow管理器
        rwfis.foreach { _.reset() }
        val allwwfis = rwfis ++ wwfis
        workflowManager = context.actorOf(Props(WorkFlowManager(wfs, allwwfis)),"wfm")
        Thread.sleep(3000)
        coordinatorManager ! GetManagers(workflowManager,coordinatorManager)
        workflowManager ! GetManagers(workflowManager,coordinatorManager)
        log.info("初始化成功")
        //若存在worker，则启动
        if(roler.size > 0){
          startActors()  
        }
		    true
    }
  }
  /**
   * 启动已经准备好的actors（必须为活动主节点）
   */
  private def startActors(){
    isStarted = true
  	coordinatorManager ! Start()
  	workflowManager ! Start()
  	xmlLoader ! Start() 
  	log.info("开始启动运行...")
  }
  /**
   * 设置为standby角色
   */
  def standby():Future[Boolean] = {
	  this.isActiveMember = false
    val rs = if(isActiveMember){
      val rF1 = (this.xmlLoader ? Stop()).mapTo[Boolean]
      var rF2 = (this.coordinatorManager ? Stop()).mapTo[Boolean]
      var rF3 = (this.workflowManager ? Stop()).mapTo[Boolean]
      val list = List(rF1, rF2, rF3)
      val rF = Future.sequence(list).map { x => if(x.filter { !_ }.size > 0) false else true }
      rF
    }else {
      Future{true}
    }
    rs
  }
  private def getDData():Future[DistributeData] = {
    for {
      wfs <- (Master.haDataStorager ? GetWorkflows()).mapTo[List[WorkflowInfo]]
      coors <- (Master.haDataStorager ? GetCoordinators()).mapTo[List[Coordinator]]
      rWFIs <- (Master.haDataStorager ? GetRWFIs()).mapTo[List[WorkflowInstance]]
	    wWFIs <- (Master.haDataStorager ? GetWWFIs()).mapTo[List[WorkflowInstance]]
	    xmlFiles <- (Master.haDataStorager ? GetXmlFiles()).mapTo[Map[String,Long]]
    } yield DistributeData(wfs,coors,rWFIs,wWFIs,xmlFiles)
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
object Master{
  def props = Props[Master]
  var persistManager:ActorRef = _
  var emailSender: ActorRef = _
  var logRecorder: ActorRef = _
  var haDataStorager: ActorRef = _
  var config:Config = _
  var system:ActorSystem = _
}