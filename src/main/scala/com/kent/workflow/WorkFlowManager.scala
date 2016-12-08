package com.kent.workflow

import akka.actor.Actor
import akka.pattern.ask
import akka.pattern.pipe
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
import com.kent.db.PersistManager.Save
import com.kent.db.PersistManager.Delete

class WorkFlowManager extends Actor with ActorLogging{
  var workflows: Map[String, WorkflowInfo] = Map()
  /**
   * Map[wfActorName, [wfName, workflowActorRef]]
   */
  var workflowActors: Map[String,Tuple2[String,ActorRef]] = Map()
  var coordinatorManager: ActorRef = _
  var persistManager: ActorRef = _
  /**
   * 增
   */
  def add(wf: WorkflowInfo): Boolean = {
println("添加工作流："+wf.name)
		persistManager ! Save(wf)
    workflows = workflows + (wf.name -> wf)
    true
  }
  /**
   * 删
   */
  def remove(name: String): Boolean = {
    persistManager ! Delete(workflows(name))
    workflows = workflows.filterNot {x => x._1 == name}.toMap
    true
  }
  /**
   * 改
   */
  def update(wf: WorkflowInfo): Boolean = {
    persistManager ! Save(wf)
    workflows = workflows.map {x => if(x._1 == wf.name) wf.name -> wf else x }.toMap
    true
  }
  /**
   * 初始化
   */
  def init(){
     ??? 
  }
  /**
   * 生成工作流实例并执行
   */
  def execute(wfName: String,params: Map[String, String]){
log.info("开始生成并执行工作流："+wfName)
    val newWfInstance = WorkflowInstance(workflows(wfName))
    newWfInstance.workflow.params = params
    //创建新的workflow actor，并加入到列表中
    val wfActorRef = context.actorOf(Props(WorkflowActor(newWfInstance, params)), newWfInstance.actorName)
    workflowActors = workflowActors + (newWfInstance.actorName -> (newWfInstance.workflow.name,wfActorRef))
    var str = "";;;;;
    workflowActors.foreach( x => str = str + x._1+" ");;;;;
    println("<WorkFlowManager> workflow实例 actor个数：" + workflowActors.size + " 分别是：" + str);;;;
    wfActorRef ! Start()
  }
  /**
   * 工作流实例完成后处理
   */
  def handleWorkFlowInstanceReply(wfInstance: WorkflowInstance):Boolean = {
    val (wfname, af) = this.workflowActors.get(wfInstance.actorName).get
    this.workflowActors = this.workflowActors.filterKeys { _ != wfInstance.actorName }.toMap
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
   * receive方法
   */
  def receive: Actor.Receive = {
    case AddWorkFlow(content) => this.add(WorkflowInfo(content))
    case RemoveWorkFlow(name) => this.remove(name)
    case UpdateWorkFlow(content) => this.update(WorkflowInfo(content))
    case NewAndExecuteWorkFlowInstance(name, params) => this.execute(name, params)
    case WorkFlowInstanceExecuteResult(wfi) => this.handleWorkFlowInstanceReply(wfi)
    case KillWorkFlowInstance(wfActorName) => this.killWorkFlowInstance(wfActorName)
    case KillWorkFlow(wfName) => this.killWorkFlow(wfName)
    case GetManagers(wfm, cm, pm) => {
      coordinatorManager = cm
      persistManager = pm
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
  case class UpdateWorkFlow(content: String)
  
  case class WorkFlowInstanceExecuteResult(workflowInstance: WorkflowInstance)
  case class WorkFlowExecuteResult(wfName: String, status: WStatus)
}