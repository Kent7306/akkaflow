package com.kent.main

import java.sql.SQLException

import akka.actor.{Actor, ActorRef, ActorSystem, OneForOneStrategy, Props}
import akka.cluster.Member
import akka.pattern.{ask, pipe}
import com.alibaba.druid.pool.DruidDataSource
import com.kent.daemon.LogRecorder
import com.kent.pub.Event._
import com.kent.pub._
import com.kent.pub.actor.{ClusterRole, Daemon}
import com.kent.pub.dao.ThreadConnector
import com.kent.workflow.ActionActor
import com.kent.workflow.node.action.ActionNodeInstance
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import akka.pattern.{ask, pipe}
import scala.util.{Failure, Success}

/**
 * worker工作节点
 */
class Worker extends ClusterRole {
  //运行中的action节点  【actioninstance_name,ar】
  var runningActionActors = Map[String, ActorRef]()

  override def supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 1, withinTimeRange = 30 second) {
    case _: SQLException => akka.actor.SupervisorStrategy.Restart
    case _: Exception => akka.actor.SupervisorStrategy.Restart
  }

  override def preStart(): Unit = {
    super.preStart()
    val config = context.system.settings.config
    //日志记录器配置
    //创建数据库连接池
    //有些行动节点是需要持久化数据的
    val cfgStr = config.getString _
    val cfgInt = config.getInt _
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
    ds.setInitialSize(cfgInt("workflow.mysql.init-size"))
    ds.setValidationQuery(cfgStr("workflow.mysql.validation-query"))
    ds.setTestOnReturn(false)
    ds.setDefaultAutoCommit(true)
    ds.setMaxWait(cfgInt("workflow.mysql.max-wait"))
    ds.setRemoveAbandoned(false)
    ds.init()
    ThreadConnector.setDataSource(ds)
  }

  override def postStop(): Unit = {
    super.postStop()
    ThreadConnector.getDataSource().close()
  }

  override def individualReceive: Actor.Receive = {
    case CreateAction(ani) => sender ! createActionActor(ani)
    case RemoveAction(name) => runningActionActors = runningActionActors - name
    case KillAllActionActor() => killAllActionActor() pipeTo sender
  }

  override def onOtherMemberUp(member: Member): Unit = {}
  override def onSelfMemberUp(): Unit = {}

  /**
    * 有其他角色退出集群
    *
    * @param member
    */
  override def onMemberRemove(member: Member): Unit = {//先留空
  }

  /**
    * 创建action actor
    */
  def createActionActor(actionNodeInstance: ActionNodeInstance): ActorRef = {
    val actionActorRef = context.actorOf(Props(ActionActor(actionNodeInstance)), actionNodeInstance.id+"_"+actionNodeInstance.hashCode())
    runningActionActors = runningActionActors + (actionNodeInstance.name -> actionActorRef)
    actionActorRef
  }

  /**
    * 杀死所有的action actor
    */
  def killAllActionActor(): Future[Boolean] = {
    val resultsF = runningActionActors.map { case (x, y) => (y ? Kill()).mapTo[ActionExecuteResult] }.toList
    val rsF = Future.sequence(resultsF).map { x =>
      this.runningActionActors = Map.empty[String, ActorRef]
      true
    }
    rsF
  }

  /**
    * 活动master通知
    *
    * @param masterRef
    * @return
    */
  override def notifyActive(masterRef: ActorRef): Future[Result] = {
    val logRecordPath = masterRef.path + "/"+Daemon.LOG_RECORDER
    Worker.activeMasterOpt = Some(masterRef)
    log.info(s"设置master引用: ${masterRef.path}")
    context.actorSelection(logRecordPath).resolveOne(20 seconds).map{ x =>
      LogRecorder.actorOpt = Some(x)
      SucceedResult()
    }
  }
}

object Worker extends App {
  import scala.collection.JavaConverters._
  private val defaultConf = ConfigFactory.load()
  private val ports = defaultConf.getIntList("workflow.node.worker.ports").asScala.toList
  var logRecorder: ActorRef = _
  var activeMasterOpt: Option[ActorRef] = None

  var config:Config = _
  ports.foreach{ port =>
    val portConf = "akka.remote.artery.canonical.port=" + port
    val portBindConf = "akka.remote.artery.bind.port=" + port
    val config = ConfigFactory.parseString(portConf)
        .withFallback(ConfigFactory.parseString(portBindConf))
        .withFallback(ConfigFactory.parseString(s"akka.cluster.roles = [${ClusterRole.WORKER}]"))
        .withFallback(defaultConf)
    Worker.config = config
    val system = ActorSystem("akkaflow", config)
    system.actorOf(Worker.props, name = ClusterRole.WORKER)
  }
  def props = Props[Worker]
}