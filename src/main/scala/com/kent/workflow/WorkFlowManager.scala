package com.kent.workflow

import akka.actor.Actor
import akka.pattern.{ ask, pipe }
import akka.actor.ActorLogging
import com.kent.coordinate.Coordinator
import akka.actor.ActorRef
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.Terminated
import akka.actor.Props
import com.kent.workflow.WorkFlowManager._
import com.kent.workflow.WorkflowInfo.WStatus._
import akka.actor.PoisonPill
import scala.concurrent.duration._
import akka.util.Timeout
import com.kent.workflow.WorkflowActor.Start
import com.kent.coordinate.CoordinatorManager.GetManagers
import com.kent.db.PersistManager._
import com.kent.main.Master._
import com.kent.workflow.node.NodeInfo.Status._
import com.kent.pub.ShareData
import com.kent.mail.EmailSender.EmailMessage
import com.kent.db.LogRecorder._
import scala.util.Success
import scala.concurrent.Future

class WorkFlowManager extends Actor with ActorLogging{
  /**
   * [wfName, workflowInfo]
   */
  var workflows: Map[String, WorkflowInfo] = Map()
  /**
   * Map[wfActorName, [wfName, workflowActorRef]]
   */
  var workflowActors: Map[String,Tuple2[String,ActorRef]] = Map()
  var coordinatorManager: ActorRef = _
  implicit val timeout = Timeout(20 seconds)
  /**
   * 初始化
   */
  init()
  /**
   * 增
   */
  def add(wf: WorkflowInfo, isPersist: Boolean): Boolean = {
    ShareData.logRecorder ! Info("WorkflowManager", null, s"添加工作流配置：${wf.name}")
		if(isPersist) ShareData.persistManager ! Save(wf)
    workflows = workflows + (wf.name -> wf)
    true
  }
  /**
   * 删
   */
  def remove(name: String): Boolean = {
    ShareData.persistManager ! Delete(workflows(name))
    workflows = workflows.filterNot {x => x._1 == name}.toMap
    true
  }
  /**
   * 初始化，从数据库中获取workflows
   */
  def init(){
    import com.kent.pub.ShareData._
    val isEnabled = config.getBoolean("workflow.mysql.is-enabled")
    if(isEnabled){
       val listF = (ShareData.persistManager ? Query("select name from workflow")).mapTo[List[List[String]]]
       listF.andThen{
         case Success(list) => list.map { x =>
           val wf = new WorkflowInfo(x(0))
           val wfF = (ShareData.persistManager ? Get(wf)).mapTo[WorkflowInfo]
           wfF.andThen{case Success(wf) => add(wf, false)}
         }
       }
    }
  }
  /**
   * 生成工作流实例并执行
   */
  def newAndExecute(wfName: String,params: Map[String, String]): Boolean = {
    if(workflows.get(wfName).isEmpty){
      ShareData.logRecorder ! Error("WorkflowManager", null, s"未找到名称为[${wfName}]的工作流")
      false
    } else {
    	val wfi = workflows(wfName).createInstance()
			wfi.parsedParams = params
			ShareData.logRecorder ! Info("WorkflowManager", null, s"开始生成并执行工作流实例：${wfi.actorName}")
			//创建新的workflow actor，并加入到列表中
			val wfActorRef = context.actorOf(Props(WorkflowActor(wfi)), wfi.actorName)
			workflowActors = workflowActors + (wfi.actorName -> (wfi.workflow.name,wfActorRef))
			wfActorRef ! Start()
			true      
    }
  }
  /**
   * 工作流实例完成后处理
   */
  def handleWorkFlowInstanceReply(wfInstance: WorkflowInstance):Boolean = {
    val (wfname, af) = this.workflowActors.get(wfInstance.actorName).get
    this.workflowActors = this.workflowActors.filterKeys { _ != wfInstance.actorName }.toMap
    //根据状态发送邮件告警
    if(wfInstance.workflow.mailLevel.contains(wfInstance.status)){
    	ShareData.emailSender ! EmailMessage(wfInstance.workflow.mailReceivers,
    	                "workflow告警", 
    	                s"任务【${wfInstance.actorName}】执行状态：${wfInstance.status}")      
    }
    ShareData.logRecorder ! Info("WorkflowInstance", wfInstance.id, s"工作流实例：${wfInstance.actorName}执行完毕，执行状态为：${wfInstance.status}")
    println("==============================")
    println(wfInstance)
    println("==============================")
    coordinatorManager ! WorkFlowExecuteResult(wfname, wfInstance.status)  
    true
  }
  /**
   * 手动kill掉工作流实例
   */
  def killWorkFlowInstance(wfaName: String): Boolean = {
    import com.kent.workflow.WorkflowActor.Kill
    if(!workflowActors.get(wfaName).isEmpty){
    	val wfaRef = workflowActors(wfaName)._2
    	wfaRef ! Kill()
    }else{
    	println(s"[WorkFlowActor：${wfaName}]不存在，不能kill掉")      
    }
    true
  }
  /**
   * 手动kill掉工作流（包含其所有的实例）
   */
  def killWorkFlow(wfName: String): Boolean = {
    workflowActors.foreach(x => if(x._2._1 == wfName){killWorkFlowInstance(x._1)})
    true
  }
  /**
   * 重跑指定的工作流实例
   */
  def reRun(wfiId: String){
    val wf = new WorkflowInfo(null)
    val wfi = wf.createInstance()
    wfi.id = "b2bdfe0c"
    implicit val timeout = Timeout(20 seconds)
    val wfiF = (ShareData.persistManager ? Get(wfi)).mapTo[WorkflowInstance]
    //重置时间与状态
    wfiF.map { x => 
      x.status = W_PREP
      x.startTime = null
      x.endTime = null
      x.nodeInstanceList.foreach { y => 
        y.status = PREP
        y.startTime = null
        y.endTime = null}
      x }.map {
         z =>
          //创建新的workflow actor，并加入到列表中
          val wfActorRef = context.actorOf(Props(WorkflowActor(z)), z.actorName)
          workflowActors = workflowActors + (z.actorName -> (z.workflow.name,wfActorRef))
          wfActorRef ! Start()
      }
  }
  /**
   * receive方法
   */
  def receive: Actor.Receive = {
    case AddWorkFlow(content) => this.add(WorkflowInfo(content), true)
    case RemoveWorkFlow(name) => this.remove(name)
    //case UpdateWorkFlow(content) => this.update(WorkflowInfo(content))
    case NewAndExecuteWorkFlowInstance(name, params) => this.newAndExecute(name, params)
    case WorkFlowInstanceExecuteResult(wfi) => this.handleWorkFlowInstanceReply(wfi)
    case KillWorkFlowInstance(wfActorName) => this.killWorkFlowInstance(wfActorName)
    case KillWorkFlow(wfName) => this.killWorkFlow(wfName)
    case ReRunWorkflowInstance(wfiId: String) => reRun(wfiId)
    case GetManagers(wfm, cm) => {
      coordinatorManager = cm
      context.watch(coordinatorManager)
    }
    case Terminated(arf) => if(coordinatorManager == arf) log.warning("coordinatorManager actor挂掉了...")
  }
}


object WorkFlowManager{
  def apply(wfs: List[WorkflowInfo]):WorkFlowManager = {
    val wfm = new WorkFlowManager;
    wfm.workflows = wfs.map { x => x.name -> x }.toMap
    wfm
  }
  def apply(contents: Set[String]):WorkFlowManager = {
    WorkFlowManager(contents.map { WorkflowInfo(_) }.toList)
  }

  
  case class NewAndExecuteWorkFlowInstance(wfName: String, params: Map[String, String])
  case class KillWorkFlow(wfName: String)
  case class KillWorkFlowInstance(wfActorName: String)
  case class AddWorkFlow(content: String)
  case class RemoveWorkFlow(wfName: String)
  case class ReRunWorkflowInstance(worflowInstanceId: String)
  case class WorkFlowInstanceExecuteResult(workflowInstance: WorkflowInstance)
  case class WorkFlowExecuteResult(wfName: String, status: WStatus)
}