package com.kent.main

import java.sql.SQLException

import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props}
import akka.cluster.{Cluster, Member}
import akka.pattern.{ask, pipe}
import com.alibaba.druid.pool.DruidDataSource
import com.kent.daemon.HaDataStorager._
import com.kent.daemon._
import com.kent.pub.actor.BaseActor.ActorInfo
import com.kent.pub.actor.BaseActor.ActorType._
import com.kent.pub.Event._
import com.kent.pub._
import com.kent.pub.actor.{ClusterRole, Daemon}
import com.kent.pub.dao.{ThreadConnector, UtilDao}
import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Random, Success}


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
    * 消息处理
    * @return
    */
  def individualReceive: Actor.Receive = {
    case AskWorker(host: String) => sender ! allocateWorker(host: String)
    case ShutdownCluster() =>  shutdownCluster(sender)
    case CollectClusterActorInfo() =>
      val sdr = sender
      collectClusterActorInfo().andThen { case Success(x) =>
        sdr ! SucceedResult("成功获取集群信息", Some(x.toMapList()))
      }
  }

  override def preStart(): Unit = {
    super.preStart()
    if (this.isActiveMember) activeInit() else standby()
  }

  override def postStop(): Unit = {
    ThreadConnector.closeDataSource()
    super.postStop()
  }

  override def onSelfMemberUp(): Unit = {
    //启动
    if(isActiveMember) log.warning("等待worker启动....")
  }
  /**
    * 注册角色节点
    * @param member
    */
  override def onOtherMemberUp(member: Member){
    val hostPortKey = member.address.host.get + ":" + member.address.port.get
    //httpserver
    operaAfterRoleMemberUp(member, ClusterRole.HTTP_SERVER,x => {
      Master.haDataStorager ! AddRole(hostPortKey, x, ClusterRole.HTTP_SERVER)
      this.httpServerOpt = Some(x)
      log.info(s"注册新角色${ClusterRole.HTTP_SERVER}节点")
      if(this.isActiveMember) {
        (x ? NotifyActive(self)).mapTo[Result]
      }
    })
   //worker
    operaAfterRoleMemberUp(member, ClusterRole.WORKER,roleActor => {
      Master.haDataStorager ! AddRole(hostPortKey, roleActor, ClusterRole.WORKER)
      workers = workers :+ roleActor
      log.info(s"注册新角色${ClusterRole.WORKER}节点，已注册数量: ${workers.size}, 当前注册${ClusterRole.WORKER}:节点路径：${roleActor}")

      if (this.isActiveMember){  //当前是活动master
        println(roleActor)
        (roleActor ? NotifyActive(self)).mapTo[Result].map{result =>

          if (!this.isActiveRunning){  //一有worker就启动
            startDaemon()
          }
        }
      }
    })
    //另一个Master启动
    operaAfterRoleMemberUp(member,ClusterRole.MASTER,x => {
      this.otherMasterOpt = Some(x)
      if (this.isActiveMember){  //启动时，如果当前是活动master
        val curMasterOptF = synGetHaMaster()
        curMasterOptF.map{
          case Some(masterRef) if masterRef.path == x.path => //目前是旧活动节点，新检查的角色是新活动节点

          case Some(maserRef) if maserRef.path != self.path => //目前是新活动节点，新检查的角色是旧活动节点
            (x ? NotifyActive(self)).mapTo[Result]
          case None =>
        }
      }
    })
  }
  /**
    * 有其他角色退出集群
    *
    * @param member
    */
  override def onMemberRemove(member: Member): Unit = {
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

    //若终止的是活动主节点,当前是备份节点,则需要激活当前
    if (member.hasRole(ClusterRole.MASTER)) {
      this.otherMasterOpt = None
      if (!this.isActiveMember){
        log.info(s"${ClusterRole.MASTER_STANDBY}节点准备切换到${ClusterRole.MASTER}节点")
        val hostPortKey = member.address.host.get + ":" + member.address.port.get
        Master.haDataStorager ! removeRole(hostPortKey)
        this.activeInit()

        //通知workers以及httpserver
        val listF = this.workers.map{ x =>
          (x ? NotifyActive(self)).mapTo[Result]
        }
        Future.sequence(listF).flatMap{l =>
          if(l.size > 0) this.startDaemon()
          if (this.httpServerOpt.isDefined){
            (this.httpServerOpt.get ? NotifyActive(self)).mapTo[Result]
          }else{
            Future{SucceedResult()}
          }
        }
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
  private def synGetHaMaster():Future[Option[ActorRef]] = {
    val rolesF = (Master.haDataStorager ? GetRoles()).mapTo[Map[String, RoleContent]]
    rolesF.map(roles => {
      val a = roles.find(_._2.roleType == ClusterRole.MASTER)
      if (a.isDefined){
        Some(a.get._2.sdr)
      }else{
        None
      }
    })
  }
  /**
   * 作为活动主节点启动
   */
  def activeInit() = {
    this.isActiveMember = true
    val config = context.system.settings.config
    val cfgStr = config.getString _
    val cfgInt = config.getInt _

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
    //创建数据库连接池
    val ds = new DruidDataSource()
    ds.setUrl(cfgStr("workflow.mysql.jdbc-url"))
    ds.setUsername(cfgStr("workflow.mysql.user"))
    ds.setPassword(cfgStr("workflow.mysql.password"))
    ds.setDriverClassName("com.mysql.jdbc.Driver")
    ds.setMaxActive(cfgInt("workflow.mysql.max-active"))
    ds.setMinIdle(cfgInt("workflow.mysql.min-idle"))
    ds.setKeepAlive(true)
    ds.setMinEvictableIdleTimeMillis(cfgInt("workflow.mysql.min-evictable-idle-time-millis"))
    ds.setTestWhileIdle(true)
    ds.setDefaultAutoCommit(true)
    ds.setInitialSize(cfgInt("workflow.mysql.init-size"))
    ds.setValidationQuery(cfgStr("workflow.mysql.validation-query"))
    ds.setTestOnReturn(false)
    ds.setMaxWait(cfgInt("workflow.mysql.max-wait"))
    ds.setRemoveAbandoned(false)
    ds.init()
    ThreadConnector.setDataSource(ds)
    //初始化表
    try {
      UtilDao.initTables()
    } catch {
      case _:Exception => log.error("执行初始化建表sql失败")
    }
    log.info(s"成功初始化数据库")

    //xmlLoader参数配置
    val xmlLoaderConfig = (config.getString("workflow.xml-loader.workflow-dir"),
      config.getInt("workflow.xml-loader.scan-interval")
    )
    //创建邮件发送器
    Master.emailSender = context.actorOf(Props(EmailSender(emailConfig._1,emailConfig._2,emailConfig._3,emailConfig._4,emailConfig._5,emailConfig._6,emailConfig._7,emailConfig._8)),Daemon.MAIL_SENDER)
    //创建日志记录器
    Master.logRecorder = context.actorOf(Props(LogRecorder()),Daemon.LOG_RECORDER)
    LogRecorder.actorOpt = Some(Master.logRecorder)
    //创建xml装载器
    Master.xmlLoader = context.actorOf(Props(XmlLoader(xmlLoaderConfig._1,xmlLoaderConfig._2)),Daemon.XML_LOADER)
    //创建workflow管理器
    Master.workflowManager = context.actorOf(Props(WorkFlowManager(List())),Daemon.WORKFLOW_MANAGER)
    //定时任务执行器
    Master.cronRunner = context.actorOf(Props(CronRunner()), Daemon.CRON_RUNNER)

    //血缘记录器
    //if(Master.lineageRecorder == null){
    //  Master.lineageRecorder = context.actorOf(Props(LineageRecorder()),"lineaage-recorder")
    //}
    val (host,port) = getHostPortKey()
    Master.haDataStorager ! AddRole(s"$host:$port", self, ClusterRole.MASTER)
  }

  def startDaemon(): Unit ={
    this.isActiveRunning = true
    Master.workflowManager ! Start()
    Master.xmlLoader ! Start()
    Master.cronRunner ! Start()
    log.info(s"当前节点角色为${ClusterRole.MASTER}，开始启动")
  }

  /**
    * 设置为standby角色
    * @return
    */
  def standby():Result = {
	  this.isActiveMember = false
    this.isActiveRunning = false
    context.children.filter(x => x.path.name != Daemon.HA_DATA_STORAGE).foreach( _ ! PoisonPill)

    //Master.setNull()
    val (host,port) = getHostPortKey()
    Master.haDataStorager ! AddRole(s"$host:$port", self, ClusterRole.MASTER_STANDBY)
    log.info(s"启动成功，角色为【${ClusterRole.MASTER_STANDBY}】")
    SucceedResult()
  }
  /**
   * 收集集群actor信息
   */
  def collectClusterActorInfo(): Future[ActorInfo] = {
    val allActorInfo = new ActorInfo()
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
    val waisF = this.workers.map { x => (x ? CollectActorInfo()).mapTo[ActorInfo]}

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
     if(Master.xmlLoader != null) Master.xmlLoader ! Stop()
     if(Master.workflowManager != null){
       val result = (Master.workflowManager ? KllAllWorkFlow()).mapTo[Result]
       result.recover{
         case e: Exception => FailedResult(s"执行出错：${e.getMessage}")
       }.andThen{ case _ =>
         workers.foreach { _ ! ShutdownCluster() }
         if(otherMasterOpt.isDefined)otherMasterOpt.get ! ShutdownCluster()
         println("worker角色与master角色已关闭")
         sdr ! SucceedResult("worker角色与master角色已关闭")
         context.system.terminate()
       }
     } else {
       context.system.terminate()
     }
  }

  /**
    * 活动master通知
    *
    * @param masterRef
    * @return
    */
  override def notifyActive(masterRef: ActorRef): Future[Result] = {
    val result = if (this.isActiveMember){
      otherMasterOpt = Some(masterRef)
      log.info(s"当前节点从活动节点变为备份节点")
      standby()
    }else {
      SucceedResult()
    }
    Future{ result}
  }
}
object Master extends App{
  def apply(isActiveMember: Boolean) = new Master(isActiveMember)
  var emailSender: ActorRef = _
  var logRecorder: ActorRef = _
  var haDataStorager: ActorRef = _
  var lineageRecorder: ActorRef = _
  var workflowManager: ActorRef = _
  var cronRunner: ActorRef = _
  var xmlLoader: ActorRef = _

  def setNull() = {
    Master.emailSender = null
    Master.logRecorder = null
    Master.workflowManager = null
    Master.cronRunner = null
    Master.xmlLoader = null
  }

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