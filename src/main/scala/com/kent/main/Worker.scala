package com.kent.main

import java.sql.SQLException

import akka.actor.{Actor, ActorRef, ActorSystem, OneForOneStrategy, Props}
import akka.pattern.{ask, pipe}
import com.kent.daemon.LogRecorder
import com.kent.pub.Event._
import com.kent.pub.actor.ClusterRole
import com.kent.workflow.ActionActor
import com.kent.workflow.node.action.ActionNodeInstance
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * worker工作节点
 */
class Worker extends ClusterRole {
	//运行中的action节点  【actioninstance_name,ar】
  var runningActionActors = Map[String,ActorRef]()
  init()
  
  override def supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 1, withinTimeRange = 30 second){
    case _:SQLException => akka.actor.SupervisorStrategy.Restart
    case _:Exception => akka.actor.SupervisorStrategy.Restart
  }
  
  def individualReceive: Actor.Receive = {
    case CreateAction(ani) => sender ! createActionActor(ani)
    case RemoveAction(name) => runningActionActors = runningActionActors - name
    case KillAllActionActor() => killAllActionActor() pipeTo sender
  }
  /**
   * 创建action actor
   */
  def createActionActor(actionNodeInstance: ActionNodeInstance):ActorRef = {
		val actionActorRef = context.actorOf(Props(ActionActor(actionNodeInstance)), actionNodeInstance.hashCode()+"")
		runningActionActors = runningActionActors + (actionNodeInstance.name -> actionActorRef)
		actionActorRef
  }
  /**
   * 杀死所有的action actor
   */
  def killAllActionActor():Future[Boolean] = {
    val resultsF = runningActionActors.map { case (x,y) => (y ? Kill()).mapTo[ActionExecuteResult] }.toList
    val rsF = Future.sequence(resultsF).map { x => 
      this.runningActionActors = Map.empty[String, ActorRef]
      true
    }
    rsF
  }
  /**
   * 初始化
   */
  def init(){
    val config = context.system.settings.config
    //日志记录器配置
    //mysql持久化参数配置
    val mysqlConfig = (config.getString("workflow.mysql.user"),
                      config.getString("workflow.mysql.password"),
                      config.getString("workflow.mysql.jdbc-url"),
                      config.getBoolean("workflow.mysql.is-enabled")
                    )
    //创建日志记录器
    Worker.logRecorder = context.actorOf(Props(LogRecorder(mysqlConfig._3,mysqlConfig._1,mysqlConfig._2,mysqlConfig._4)),"log-recorder")
    LogRecorder.actor = Worker.logRecorder
  }
}

object Worker extends App {
  import scala.collection.JavaConverters._
  val defaultConf = ConfigFactory.load()
  val ports = defaultConf.getIntList("workflow.node.worker.ports").asScala.toList
  var logRecorder: ActorRef = _
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
    val worker = system.actorOf(Worker.props, name = ClusterRole.WORKER)
  }
  def props = Props[Worker]
}