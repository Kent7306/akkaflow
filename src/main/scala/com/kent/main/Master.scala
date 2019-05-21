package com.kent.main

import java.sql.SQLException

import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props}
import akka.cluster.{Cluster, Member}
import akka.pattern.{ask, pipe}
import com.kent.daemon.HaDataStorager._
import com.kent.daemon._
import com.kent.pub.actor.BaseActor.ActorInfo
import com.kent.pub.actor.BaseActor.ActorType._
import com.kent.pub.Event._
import com.kent.pub.actor.{ClusterRole, Daemon}
import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.{Random, Success}


class Master(var isActiveMember:Boolean) extends ClusterRole {
  var httpServerOpt: Option[ActorRef] = None
  var workers = List[ActorRef]()
  //其他
  var otherMasterOpt:Option[ActorRef] = None
  //集群状态
  var isActiveRunning = false
  /**
   * 监控策略  
   */
  import akka.actor.OneForOneStrategy
  override def supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 1, withinTimeRange = 30 second){
    case _:SQLException => akka.actor.SupervisorStrategy.Restart
    case _:Exception => akka.actor.SupervisorStrategy.Restart
  }

  //创建集群高可用数据分布式数据寄存器
  Master.haDataStorager = context.actorOf(Props[HaDataStorager],"ha-data")
  /**
   * 当集群节点可用时
   */
  Cluster(context.system).registerOnMemberUp({
    //启动
    if(!isActiveMember) standby()
    else log.warning("等待worker启动....")
  })

  /**
    * 消息处理
    * @return
    */
  def individualReceive: Actor.Receive = {
    case AskWorker(host: String) => sender ! allocateWorker(host: String)
    case ShutdownCluster() =>  shutdownCluster(sender)
    case CollectClusterActorInfo() =>
      val sdr = sender
      collectClusterActorInfo().andThen { case Success(x) =>
        sdr ! ResponseData("success","成功获取集群信息", x.toMapList())
      }
  }

  /**
    * 注册角色节点
    * @param member
    */
  override def onRoleMemberUp(member: Member){
    val hostPortKey = member.address.host.get + ":" + member.address.port.get
    //httpserver
    operaAfterRoleMemberUp(member, ClusterRole.HTTP_SERVER,x => {
      Master.haDataStorager ! AddRole(hostPortKey, x, ClusterRole.HTTP_SERVER)
      //注册
      this.httpServerOpt = Some(x)
      log.info(s"注册新角色${ClusterRole.HTTP_SERVER}节点")
      if(this.isActiveMember) {
        val resultF = (x ? NotifyActive(self)).mapTo[Result]
        Await.result(resultF, 10 second)
      }
    })
   //worker
    operaAfterRoleMemberUp(member, ClusterRole.WORKER,roleActor => {
      Master.haDataStorager ! AddRole(hostPortKey, roleActor, ClusterRole.WORKER)
      workers = workers :+ roleActor
      log.info(s"注册新角色${ClusterRole.WORKER}节点，已注册数量: ${workers.size}, 当前注册${ClusterRole.WORKER}:节点路径：${roleActor}")
  	  if (this.isActiveMember){
        val resultF = (roleActor ? NotifyActive(self)).mapTo[Result]
        Await.result(resultF, 10 second)
        active()
      }
    })
    //另一个Master启动
    operaAfterRoleMemberUp(member,ClusterRole.MASTER,x => {
      if(x != self){
        this.otherMasterOpt = Some(x)
        //启动时，如果当前是主master，则通知另外一个master
        if (this.isActiveMember){
          val curMasterOpt = synGetHaMaster()
          if (curMasterOpt.isDefined && curMasterOpt.get.path == x.path){
            val resultF = (x ? NotifyActive(self)).mapTo[Result]
            Await.result(resultF, 10 second)
            active()
          }
        }
		  }
    })
  }
  /**
    * 有其他角色退出集群
    *
    * @param member
    */
  override def onRoleMemberRemove(member: Member): Unit = {
    //若是worker，则删除
    if (member.hasRole(ClusterRole.WORKER)){
      val path = this.getRoleActorPath(member, ClusterRole.WORKER).get
      workers = workers.filter(x => {
        if (x.path == path){
          log.info(s"${ClusterRole.WORKER}:${path.address.host}被移除")
          false
        } else true
      })
    }

    //若终止的是活动主节点，则需要激活当前备份节点
    if (member.hasRole(ClusterRole.MASTER)) {
      this.otherMasterOpt = None
      if (!this.isActiveMember){
        log.info(s"${ClusterRole.MASTER_STANDBY}节点准备切换到${ClusterRole.MASTER}节点")
        val hostPortKey = member.address.host.get + ":" + member.address.port.get
        Master.haDataStorager ! removeRole(hostPortKey)
        this.active()


        //通知workers
        val listF = this.workers.map{ x =>
          (x ? NotifyActive(self)).mapTo[Result]
        }
        val resultF = Future.sequence(listF).flatMap{x =>
          //通知httpserver
          if (this.httpServerOpt.isDefined){
            (this.httpServerOpt.get ? NotifyActive(self)).mapTo[Result]
          }else{
            Future{Result(true, "", None)}
          }
        }
        Await.result(resultF,20 second)
      }
    }
  }

  /**
    * 请求得到新的worker，动态分配
    * @param host
    * @return
    */
  private def allocateWorker(host: String):Option[ActorRef] = {
    if(workers.size > 0) {
      //host为-1情况下，随机分配
      if(host == "-1") {
        Some(workers(Random.nextInt(workers.size)))
      }else{ //指定host分配
      	val list = workers.filter { _.path.address.host.get == host }.toList
      	if(list.size > 0)
      	  Some(workers(Random.nextInt(list.size))) else None
      }
    }else{
      None
    }
  }

  /**
    * 获取当前集群的活动master
    * @return
    */
  private def synGetHaMaster():Option[ActorRef] = {
    val rolesF = (Master.haDataStorager ? GetRoles()).mapTo[Map[String, RoleContent]]
    val masterOptF = rolesF.map(roles => {
      val a = roles.find(_._2.roleType == ClusterRole.MASTER)
      if (a.isDefined){
        Some(a.get._2.sdr)
      }else{
        None
      }
    })
    Await.result(masterOptF, 20 second)
  }
  /**
   * 作为活动主节点启动
   */
  def active() = {
    //满足条件才能激活
    if (!this.isActiveRunning && this.workers.nonEmpty) {
      //判断是否存在旧活动master
      val isActiveMasterExist = if(synGetHaMaster().isDefined) true else false
      //原master运行中，新master接替，原master变成备份master
      //集群初始化，master直接启动
      if ((isActiveMasterExist && this.otherMasterOpt.isDefined) || !isActiveMasterExist){
        this.isActiveRunning = true
        this.isActiveMember = true
        val config = context.system.settings.config
        //mysql持久化参数配置
        val mysqlConfig = (config.getString("workflow.mysql.user"),
          config.getString("workflow.mysql.password"),
          config.getString("workflow.mysql.jdbc-url")
        )
        //Email参数配置
        val isHasPort = config.hasPath("workflow.email.smtp-port")
        val isHasNickName = config.hasPath("workflow.email.nickname")
        val account = config.getString("workflow.email.account")
        val mailPortOpt = if(isHasPort) Some(config.getInt("workflow.email.smtp-port")) else None
        val nickName = if (isHasNickName) config.getString("workflow.email.nickname") else account
        val emailConfig = (
          config.getString("workflow.email.hostname"),
          mailPortOpt,
          config.getBoolean("workflow.email.auth"),
          account,
          nickName,
          config.getString("workflow.email.password"),
          config.getString("workflow.email.charset"),
          config.getBoolean("workflow.email.is-enabled")
        )
        //xmlLoader参数配置
        val xmlLoaderConfig = (config.getString("workflow.xml-loader.workflow-dir"),
          config.getInt("workflow.xml-loader.scan-interval")
        )
        //创建数据库连接器
        Master.dbConnector = context.actorOf(Props(DbConnector(mysqlConfig._3,mysqlConfig._1,mysqlConfig._2)),Daemon.DB_CONNECTOR)

        //创建邮件发送器
        Master.emailSender = context.actorOf(Props(EmailSender(emailConfig._1,emailConfig._2,emailConfig._3,emailConfig._4,emailConfig._5,emailConfig._6,emailConfig._7,emailConfig._8)),Daemon.MAIL_SENDER)
        //创建日志记录器
        Master.logRecorder = context.actorOf(Props(LogRecorder(mysqlConfig._3,mysqlConfig._1,mysqlConfig._2)),Daemon.LOG_RECORDER)
        LogRecorder.actor = Master.logRecorder
        //创建xml装载器
        Master.xmlLoader = context.actorOf(Props(XmlLoader(xmlLoaderConfig._1,xmlLoaderConfig._2)),Daemon.XML_LOADER)
        //创建workflow管理器
        Master.workflowManager = context.actorOf(Props(WorkFlowManager(List())),Daemon.WORKFLOW_MANAGER)
        //血缘记录器
        //if(Master.lineageRecorder == null){
        //  Master.lineageRecorder = context.actorOf(Props(LineageRecorder()),"lineaage-recorder")
        //}
        val (host,port) = getHostPortKey()
        Master.haDataStorager ! AddRole(s"$host:$port", self, ClusterRole.MASTER)
        Master.workflowManager ! Start()
        Master.xmlLoader ! Start()
        log.info(s"当前节点角色为${ClusterRole.MASTER}，已启动成功")
      }


    }
  }

  /**
    * 设置为standby角色
    * @return
    */
  def standby():Result = {
	  this.isActiveMember = false
    this.isActiveRunning = false
    context.children.filter(x => x.path.name != Daemon.HA_DATA_STORAGE).foreach( _ ! PoisonPill)

    val (host,port) = getHostPortKey()
    Master.haDataStorager ! AddRole(s"$host:$port", self, ClusterRole.MASTER_STANDBY)
    log.info(s"启动成功，角色为【${ClusterRole.MASTER_STANDBY}】")
    Result(true, "", None)
  }
  /**
   * 收集集群actor信息
   */
  def collectClusterActorInfo(): Future[ActorInfo] = {
    var allActorInfo = new ActorInfo()
    allActorInfo.name = "top"
    allActorInfo.atype = ROLE
    //获取本master的信息
    val maiF = (self ? CollectActorInfo()).mapTo[ActorInfo]
    //获取另外备份master的信息
    val omaiF = if(otherMasterOpt.isDefined){
      (otherMasterOpt.get ? CollectActorInfo()).mapTo[ActorInfo]
    }else{
      null
    }
    //获取worker的actor信息
    val waisF = this.workers.map { x => (x ? CollectActorInfo()).mapTo[ActorInfo]}.toList

    val allAIsF = if(omaiF == null){
      waisF ++ (maiF  :: Nil)
    }else{
      waisF ++ (maiF :: omaiF :: Nil)
    }
    val allAiF = Future.sequence(allAIsF).map{ list =>
      allActorInfo.subActors = allActorInfo.subActors ++ list
      allActorInfo
    }
    allAiF
  }

  /**
    * 关闭集群
    * @param sdr
    * @return
    */
  def shutdownCluster(sdr: ActorRef) = {
     if(Master.xmlLoader!=null) Master.xmlLoader ! Stop()
     if(Master.workflowManager!=null){
       val result = (Master.workflowManager ? KllAllWorkFlow()).mapTo[ResponseData]
       result.andThen{
         case Success(x) =>
           workers.foreach { _ ! ShutdownCluster() }
           if(otherMasterOpt.isDefined)otherMasterOpt.get ! ShutdownCluster()
           context.system.terminate()
      }
     }else{
       context.system.terminate()
     }
     sdr ! ResponseData("success","worker角色与master角色已关闭",null)
  }

  /**
    * 活动master通知
    *
    * @param masterRef
    * @return
    */
  override def notifyActive(masterRef: ActorRef): Result = {
    log.info(s"设置master引用: ${masterRef.path}")
    if (this.isActiveMember){
      standby()
    }else {
      Result(true,"",None)
    }
  }
}
object Master extends App{
  def apply(isActiveMember: Boolean) = new Master(isActiveMember)
  var emailSender: ActorRef = _
  var logRecorder: ActorRef = _
  var haDataStorager: ActorRef = _
  var lineageRecorder: ActorRef = _
  var dbConnector: ActorRef = _
  var workflowManager: ActorRef = _
  var xmlLoader: ActorRef = _

  startUp()
  /**
   * 作为活动主节点启动（若已存在活动主节点，则把该节点设置为备份主节点）
   */
  def startUp(): Unit = {
      val defaultConf = ConfigFactory.load()
      val port = defaultConf.getInt("workflow.node.master.port")
      val portConf = "akka.remote.artery.canonical.port=" + port
      val portBindConf = "akka.remote.artery.bind.port=" + port

      // 创建一个Config对象
      val config = ConfigFactory.parseString(portConf)
          .withFallback(ConfigFactory.parseString(portBindConf))
          .withFallback(ConfigFactory.parseString(s"akka.cluster.roles = [${ClusterRole.MASTER}]"))
          .withFallback(defaultConf)
      // 创建一个ActorSystem实例
      val system = ActorSystem("akkaflow", config)
      val master = system.actorOf(Props(Master(true)), name = ClusterRole.MASTER)
  }
  
}