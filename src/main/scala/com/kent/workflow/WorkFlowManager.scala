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
import com.kent.main.HttpServer.ResponseData
import scala.concurrent.Await

class WorkFlowManager extends Actor with ActorLogging{
  /**
   * [wfName, workflowInfo]
   */
  var workflows: Map[String, WorkflowInfo] = Map()
  /**
   * Map[WorflowInstance.id, [wfname, workflowActorRef]]
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
  def add(content: String, isSaved: Boolean): ResponseData = {
    var wf:WorkflowInfo = null
    try {
    	wf = WorkflowInfo(content)      
    } catch{
      case e: Exception => e.printStackTrace()
      return ResponseData("fail","content解析错误", null)
    }
    add(wf, isSaved)
  }
  def add(wf: WorkflowInfo, isSaved: Boolean): ResponseData = {
    ShareData.logRecorder ! Info("WorkflowManager", null, s"添加工作流配置：${wf.name}")
		if(isSaved) ShareData.persistManager ! Save(wf)
		
		if(workflows.get(wf.name).isEmpty){
			workflows = workflows + (wf.name -> wf)
		  ResponseData("success",s"成功添加工作流${wf.name}", null)
		}else{
		  workflows = workflows + (wf.name -> wf)
		  ResponseData("success",s"成功替换工作流${wf.name}", null)
		}
  }
  /**
   * 删
   */
  def remove(name: String): ResponseData = {
    if(!workflows.get(name).isEmpty){
      ShareData.logRecorder ! Info("WorkflowManager", null, s"删除工作流：${name}")
    	ShareData.persistManager ! Delete(workflows(name))
    	workflows = workflows.filterNot {x => x._1 == name}.toMap
      ResponseData("success",s"成功删除工作流${name}", null)
    }else{
      ResponseData("fail",s"工作流${name}不存在", null)
    }
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
           val wfF = (ShareData.persistManager ? Get(wf)).mapTo[Option[WorkflowInfo]]
           wfF.andThen{
             case Success(wfOpt) => 
             if(!wfOpt.isEmpty) add(wfOpt.get, false)
           }
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
			workflowActors = workflowActors + (wfi.id -> (wfi.workflow.name,wfActorRef))
			wfActorRef ! Start()
			true      
    }
  }
  /**
   * 工作流实例完成后处理
   */
  def handleWorkFlowInstanceReply(wfInstance: WorkflowInstance):Boolean = {
    val (wfname, af) = this.workflowActors.get(wfInstance.id).get
    this.workflowActors = this.workflowActors.filterKeys { _ != wfInstance.id }.toMap
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
  def killWorkFlowInstance(id: String): ResponseData = {
    import com.kent.workflow.WorkflowActor.Kill
    if(!workflowActors.get(id).isEmpty){
    	val wfaRef = workflowActors(id)._2
    	wfaRef ! Kill()
    	ResponseData("success",s"开始执行杀掉工作流[${id}]", null)
    }else{
      ResponseData("fail",s"[工作流实例：${id}]不存在，不能kill掉", null)
    }
  }
  /**
   * 手动kill掉工作流（包含其所有的实例）
   */
  def killWorkFlow(wfName: String): ResponseData = {
    workflowActors.foreach(x => if(x._2._1 == wfName){killWorkFlowInstance(x._1)})
    ResponseData("success",s"开始执行杀掉工作流[wfName]的所有实例", null)
  }
  /**
   * 重跑指定的工作流实例
   */
  def reRun(wfiId: String): ResponseData = {
    val wf = new WorkflowInfo(null)
    val wfi = wf.createInstance()
    wfi.id = wfiId
    implicit val timeout = Timeout(20 seconds)
    val wfiF = (ShareData.persistManager ? Get(wfi)).mapTo[Option[WorkflowInstance]]
    //该工作流实例不存在, 这里用了阻塞
    val wfiOpt = Await.result(wfiF, 20 second)
    if(wfiOpt.isEmpty){
      ResponseData("fail", s"工作流实例[${wfiId}]不存在", null)
    }else{
      //重置时间与状态
    	val wfi2 = wfiOpt.get
      wfi2.status = W_PREP
      wfi2.startTime = null
      wfi2.endTime = null
      wfi2.nodeInstanceList.foreach { y =>  y.status = PREP; y.startTime = null; y.endTime = null}
      
      //创建新的workflow actor，并加入到列表中
      val wfActorRef = context.actorOf(Props(WorkflowActor(wfi2)), wfi2.actorName)
      workflowActors = workflowActors + (wfi2.id -> (wfi2.workflow.name,wfActorRef))
      wfActorRef ! Start()
      ResponseData("success", s"工作流实例[${wfiId}]开始重跑", null)
    }
  }
  /**
   * receive方法
   */
  def receive: Actor.Receive = {
    case AddWorkFlow(content) => sender ! this.add(content, true)
    case RemoveWorkFlow(name) => sender ! this.remove(name)
    //case UpdateWorkFlow(content) => this.update(WorkflowInfo(content))
    case NewAndExecuteWorkFlowInstance(name, params) => this.newAndExecute(name, params)
    case WorkFlowInstanceExecuteResult(wfi) => this.handleWorkFlowInstanceReply(wfi)
    case KillWorkFlowInstance(id) => sender ! this.killWorkFlowInstance(id)
    case KillWorkFlow(wfName) => sender ! this.killWorkFlow(wfName)
    case ReRunWorkflowInstance(wfiId: String) => sender ! reRun(wfiId)
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
  case class KillWorkFlowInstance(id: String)
  case class AddWorkFlow(content: String)
  case class RemoveWorkFlow(wfName: String)
  case class ReRunWorkflowInstance(worflowInstanceId: String)
  case class WorkFlowInstanceExecuteResult(workflowInstance: WorkflowInstance)
  case class WorkFlowExecuteResult(wfName: String, status: WStatus)
}