package com.kent.workflow

import akka.actor.Actor
import akka.pattern.{ ask, pipe }
import akka.actor.ActorLogging
import com.kent.coordinate.Coordinator
import akka.actor.ActorRef
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.Terminated
import akka.actor.Props
import com.kent.workflow.WorkflowInfo.WStatus._
import akka.actor.PoisonPill
import scala.concurrent.duration._
import akka.util.Timeout
import com.kent.workflow.node.NodeInfo.Status._
import com.kent.main.Master
import com.kent.pub.Event._
import scala.util.Success
import scala.concurrent.Future
import scala.concurrent.Await
import org.json4s.Extraction
import org.json4s.jackson.JsonMethods
import org.json4s.DefaultFormats

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
  //init()
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
  /**
   * 增
   */
  def add(wf: WorkflowInfo, isSaved: Boolean): ResponseData = {
    Master.logRecorder ! Info("WorkflowManager", null, s"添加工作流配置：${wf.name}")
		if(isSaved) Master.persistManager ! Save(wf)
		
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
      Master.logRecorder ! Info("WorkflowManager", null, s"删除工作流：${name}")
    	Master.persistManager ! Delete(workflows(name))
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
    import com.kent.main.Master._
    val isEnabled = config.getBoolean("workflow.mysql.is-enabled")
    if(isEnabled){
       val listF = (Master.persistManager ? Query("select name from workflow")).mapTo[List[List[String]]]
       listF.andThen{
         case Success(list) => list.map { x =>
           val wf = new WorkflowInfo(x(0))
           val wfF = (Master.persistManager ? Get(wf)).mapTo[Option[WorkflowInfo]]
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
      Master.logRecorder ! Error("WorkflowManager", null, s"未找到名称为[${wfName}]的工作流")
      false
    } else {
    	val wfi = workflows(wfName).createInstance()
			wfi.parsedParams = params
			Master.logRecorder ! Info("WorkflowManager", null, s"开始生成并执行工作流实例：${wfi.actorName}")
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
    //剔除该完成的工作流实例
    val (wfname, af) = this.workflowActors.get(wfInstance.id).get
    this.workflowActors = this.workflowActors.filterKeys { _ != wfInstance.id }.toMap
    //根据状态发送邮件告警
    if(wfInstance.workflow.mailLevel.contains(wfInstance.status)){
    	Master.emailSender ! EmailMessage(wfInstance.workflow.mailReceivers,
    	                "workflow告警", 
    	                s"任务【${wfInstance.actorName}】执行状态：${wfInstance.status}")      
    }
    Thread.sleep(1000)
    Master.logRecorder ! Info("WorkflowInstance", wfInstance.id, s"工作流实例：${wfInstance.actorName}执行完毕，执行状态为：${wfInstance.status}")
    println("WorkflowInstance", wfInstance.id, s"工作流实例：${wfInstance.actorName}执行完毕，执行状态为：${wfInstance.status}")
    coordinatorManager ! WorkFlowExecuteResult(wfname, wfInstance.status)  
    true
  }
  /**
   * 手动kill掉工作流实例
   */
  def killWorkFlowInstance(id: String): Future[ResponseData] = {
    if(!workflowActors.get(id).isEmpty){
    	val wfaRef = workflowActors(id)._2
    	val result = (wfaRef ? Kill()).mapTo[List[ActionExecuteResult]]
    	val resultF = result.map { x => 
    	  implicit val formats = DefaultFormats
        val strTmp = JsonMethods.compact(Extraction.decompose(x))
    	  ResponseData("success",s"工作流[${id}]已被杀死", strTmp) 
    	}
    	resultF
    }else{
      Future(ResponseData("fail",s"[工作流实例：${id}]不存在，不能kill掉", null))
    }
  }
  /**
   * 手动kill掉工作流（包含其所有的实例）
   */
  def killWorkFlow(wfName: String): Future[ResponseData] = {
    val result = workflowActors.filter(_._2._1 == wfName).map(x => killWorkFlowInstance(x._1)).toList
    val resultF = Future.sequence(result).map { x => 
      ResponseData("success",s"工作流名称[wfName]的所有实例已经被杀死", null) 
    }
   resultF 
  }
  /**
   * 手动kill所有工作流
   */
  def killAllWorkFlow(): Future[ResponseData] = {
    val result = workflowActors.map(x => killWorkFlowInstance(x._1)).toList
    val resultF = Future.sequence(result).map { x => 
      ResponseData("success",s"所有工作流实例已经被杀死", null) 
      
    }
   resultF 
  }
  /**
   * 重跑指定的工作流实例
   */
  def reRun(wfiId: String): ResponseData = {
    val wf = new WorkflowInfo(null)
    val wfi = wf.createInstance()
    wfi.id = wfiId
    implicit val timeout = Timeout(20 seconds)
    val wfiF = (Master.persistManager ? Get(wfi)).mapTo[Option[WorkflowInstance]]
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
   * 收集集群信息
   */
  def collectClusterInfo(): ActorInfo = {
    import com.kent.pub.Event.ActorType._
    val ai = new ActorInfo()
    ai.name = self.path.name
    ai.atype = DEAMO
    ai.subActors = context.children.map { x =>
      val aiTmp = new ActorInfo()
      aiTmp.name = x.path.name
      aiTmp.atype = ACTOR
      aiTmp
    }.toList
    ai
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
    case KillWorkFlowInstance(id) =>  val sdr = sender
                                      this.killWorkFlowInstance(id).andThen{
                                        case Success(x) => sdr ! x
                                      }
    case KllAllWorkFlow() => val sdr = sender
                             this.killAllWorkFlow().andThen{
                                case Success(x) => sdr ! x
                              }
    case KillWorkFlow(wfName) => val sdr = sender
                                 this.killWorkFlow(wfName).andThen{
                                        case Success(x) => sdr ! x
                                      }
    case ReRunWorkflowInstance(wfiId: String) => sender ! reRun(wfiId)
    case GetManagers(wfm, cm) => {
      coordinatorManager = cm
      context.watch(coordinatorManager)
    }
    case CollectClusterInfo() => sender ! GetClusterInfo(collectClusterInfo())
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
}