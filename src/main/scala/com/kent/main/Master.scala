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

class Master extends ClusterRole {
  var coordinatorManager: ActorRef = _
  var workflowManager: ActorRef = _
  
  def receive: Actor.Receive = {
    case MemberUp(member) => 
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
      workers = workers :+ sender
      log.info("Interceptor registered: " + sender)
      log.info("Registered interceptors: " + workers.size)
    }
    case Start() => this.start()
    case Terminated(workerActorRef) =>
      //worker终止，更新缓存的ActorRef
      workers = workers.filterNot(_ == workerActorRef)
    case AddWorkFlow(wfStr) => workflowManager ! AddWorkFlow(wfStr)
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
      workers(Random.nextInt(workers.size))
    }else{ //指定host分配
    	val list = workers.map { x => x.path.address.host.get }.toList
    	if(list.size > 0){
    		workers(Random.nextInt(list.size))       	  
    	}else{
    	  null
    	}
    }
  }
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
  val system = ActorSystem("workflow-system", config)
  val master = system.actorOf(Master.props, name = "master")
  
  master ! Start()
  
  val coorStr_win = """
	  <coordinator name="coor1_1" start="2016-09-10 10:00:00" end="2017-09-10 10:00:00" id="0001">    
        <trigger>
            <cron config="* * * * * *"/>
        </trigger>
        <workflow-list>   
          <workflow path="wf_join_1"></workflow>
        </workflow-list>
        <param-list>
            <param name="yestoday" value="${time.today|yyyy-MM-dd|-1 day}"/>
            <param name="lastMonth" value="${time.today|yyyyMM|-1 month}"/>
            <param name="yestoday2" value="${time.yestoday}"/>
        </param-list>
    </coordinator>
	    """
  val coorStr_win2 = """
    <coordinator name="coor1_2" start="2016-09-10 10:00:00" end="2017-09-10 10:00:00" id="0002">    
        <trigger>
            <cron config="* * * * * *"/>
            <depend-list>
              <depend wf="wf_join_1" />
              <depend wf="wf_join_2" />
            </depend-list>
        </trigger>
        <workflow-list>
          <workflow path="wf_join_1"></workflow>
        </workflow-list>
        <param-list>
            <param name="yestoday" value="${time.today|yyyy-MM-dd|-1 day}"/>
            <param name="lastMonth" value="${time.today|yyyyMM|-1 month}"/>
            <param name="yestoday2" value="${time.yestoday}"/>
        </param-list>
    </coordinator>
    
    """
  val coorStr_mac = """
	     <coordinator name="coor1_1" start="2016-09-10 10:00:00" end="2017-09-10 10:00:00">    
        <trigger>
            <cron config="* * * * * *"/>
        </trigger>
        <workflow-list>
          <workflow path="wf_join"></workflow>
        </workflow-list>
        <param-list>
            <param name="yestoday" value="${time.today|yyyy-MM-dd|-1 day}"/>
            <param name="lastMonth" value="${time.today|yyyyMM|-1 month}"/>
            <param name="yestoday2" value="${time.yestoday}"/>
        </param-list>
    </coordinator>
	    """
    
    val wfStr_win_1 = """
      <work-flow name="wf_join_1" id="c80c53" mail-level = "W_SUCCESSED,W_FAILED,W_KILLED" 
        mail-receivers="15018735011@163.com,492005267@qq.com">
          <start name="start_node_1" to="fork_node" />
          <fork name="fork_node">
              <path to="action_node_2" />
          </fork> 
          <action name="action_node_2" retry-times="5" interval="10" timeout="500" host="127.0.0.1" desc = "这是节点测试">
              <file-watcher>
                <file dir="F://1111" num-threshold="1">*.txt</file>
                <size-warn-message enable="true" size-threshold="1532MB"></size-warn-message>  
              </file-watcher>
              <ok to="join_node"/>
              <error to="join_node"/>
          </action>
          <kill name="kill_node">
              <message>kill by node(被kill node杀掉了)</message>
          </kill>
          <join name="join_node" to="end_node"/>
          <end name="end_node"/>
      </work-flow>
      """
    val wfStr_win_2 = """
      <work-flow name="wf_join_2" id="a80c54">
          <start name="start_node_1" to="fork_node" />
          <fork name="fork_node">
              <path to="action_node_1" />
              <path to="action_node_2" />
          </fork>
          <action name="action_node_1" retry-times="3" interval="2" timeout="500" host="127.0.0.1" desc = "这是节点测试">
              <shell>
                  <command>D://Strawberry//perl//bin//perl F://test.pl</command>
              </shell>
              <ok to="join_node"/>
              <error to="join_node"/>
          </action>
          <action name="action_node_2" retry-times="1" interval="3" timeout="500">
              <shell>
                  <command>D://Strawberry//perl//bin//perl F://test2.pl</command>
              </shell>
              <ok to="join_node"/>
              <error to="join_node"/>
          </action>
          <kill name="kill_node">
              <message>kill by node(被kill node杀掉了)</message>
          </kill>
          <join name="join_node" to="end_node"/>
          <end name="end_node"/>
      </work-flow>
      """
    val wfStr_mac = """
      <work-flow name="wf_join" id="a80c53" mail-level = "W_SUCCESSED,W_FAILED,W_KILLED" 
        mail-receivers="15018735011@163.com,492005267@qq.com">
          <start name="start_node" to="fork_node" />
          <fork name="fork_node">
              <path to="action_node_1" />
          </fork>
          <action name="action_node_1" retry-times="5" interval="30" timeout="500" host="127.0.0.1" desc = "这是节点测试">
              <file-watcher>
                <file dir="/Users/kent/Documents/tmp" num-threshold="1">*.sh</file>
                <size-warn-message enable="true" size-threshold="2MB">
                <![CDATA[
                  文件容量小于1532M，请联系xxx进行确认
                ]]>
                </size-warn-message>  
              </file-watcher>
              <ok to="join_node"/>
              <error to="kill-node"/>
          </action>
          <kill name="kill_node">
              <message>kill by node(被kill node杀掉了)</message>
          </kill>
          <join name="join_node" to="end_node"/>
          <end name="end_node"/>
      </work-flow>
      """
    
    Thread.sleep(10000)
//  master ! ReRunWorkflowInstance("b2bdfe0c")
//    
//  master ! AddWorkFlow(wfStr_win_1)
 // master ! AddWorkFlow(wfStr_win_2)
  //  master ! AddCoor(coorStr_win) 
 // master ! AddCoor(coorStr_win2) 
  
 //   master ! AddWorkFlow(wfStr_mac)
    master ! AddCoor(coorStr_mac) 
}