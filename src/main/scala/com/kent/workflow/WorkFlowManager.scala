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
import java.util.Date
import akka.actor.Cancellable
import com.kent.ddata.HaDataStorager._
import com.kent.pub.ActorTool
import com.kent.pub.DaemonActor
import com.kent.db.LogRecorder.LogType
import com.kent.db.LogRecorder.LogType._
import com.kent.db.LogRecorder
import java.io.File
import scala.io.Source
import com.kent.util.FileUtil
import com.kent.workflow.WorkflowInfo.WStatus
import com.kent.util.Util

class WorkFlowManager extends DaemonActor {
  /**
   * 工作流信息
   * [wfName, workflowInfo]
   */
  var workflows: Map[String, WorkflowInfo] = Map()
  /**
   * 运行中的工作流实例actor集合
   * Map[WorflowInstance.id, [WorkfowInstance, workflowActorRef]]
   */
  var workflowActors: Map[String, Tuple2[WorkflowInstance, ActorRef]] = Map()
  /**
   * 等待队列
   */
  val waittingWorkflowInstance = scala.collection.mutable.ListBuffer[WorkflowInstance]()

  var coordinatorManager: ActorRef = _
  //调度器
  var scheduler: Cancellable = _
  
  /**
   * 初始化(从数据库中读取工作流xml，同步阻塞方法)
   */
  def init(){
     val rsF = (Master.persistManager ? Query("select xml_str from workflow")).mapTo[List[List[String]]]
     val listF = rsF.map{ list =>
       list.filter { _.size > 0 }.map { l => this.add(l(0), true) }.toList
     }
     val list = Await.result(listF, 20 second)
     list.map { 
       case x if x.result == "success" => 
         log.info(s"解析数据库的工作流: ${x.msg}")
       case x if x.result == "error" => 
         log.error(s"解析数据库的工作流: ${x.msg}")
     }
  }
  
  /**
   * 启动
   */
  def start(): Boolean = {
    init()
    LogRecorder.info(WORKFLOW_MANAGER, null, null, s"启动扫描...")
    this.scheduler = context.system.scheduler.schedule(200 millis, 2000 millis) {
      self ! Tick()
    }
    true
  }
  /**
   * 扫描等待队列
   */
  def tick() {
    //从等待队列中找到满足运行的工作流实例
    def getSatisfiedWFIfromWaitingWFIs(): Option[WorkflowInstance] = {
      for (wfi <- waittingWorkflowInstance) {
        val runningInstanceNum = workflowActors.map { case (x, (y, z)) => y }
          .filter { _.workflow.name == wfi.workflow.name }.size
        if (runningInstanceNum < wfi.workflow.instanceLimit) {
          waittingWorkflowInstance -= wfi
          Master.haDataStorager ! RemoveWWFI(wfi.id)
          return Some(wfi)
        }
      }
      None
    }
    val wfiOpt = getSatisfiedWFIfromWaitingWFIs()
    if (!wfiOpt.isEmpty) {
      val wfi = wfiOpt.get
      val wfActorRef = context.actorOf(Props(WorkflowActor(wfi)), wfi.actorName)
      LogRecorder.info(WORKFLOW_MANAGER, wfi.id, wfi.workflow.name, s"开始生成并执行工作流实例：${wfi.actorName}")
      workflowActors = workflowActors + (wfi.id -> (wfi, wfActorRef))
      Master.haDataStorager ! AddRWFI(wfi)
      wfActorRef ! Start()
    }
  }

  def stop(): Boolean = {
    if (scheduler == null || scheduler.isCancelled) true else scheduler.cancel()
  }

  /**
   * 增
   */
  def add(xmlStr: String, isSaved: Boolean): ResponseData = {
    var wf: WorkflowInfo = null
    try {
      wf = WorkflowInfo(xmlStr)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        return ResponseData("fail", "xml解析错误", e.getMessage)
    }
    add(wf, isSaved)
  }
  def add(wf: WorkflowInfo, isSaved: Boolean): ResponseData = {
    LogRecorder.info(WORKFLOW_MANAGER, null, wf.name, s"配置添加工作流：${wf.name}")
    if (isSaved) Master.persistManager ! Save(wf)
    Master.haDataStorager ! AddWorkflow(wf)

    if (workflows.get(wf.name).isEmpty) {
      workflows = workflows + (wf.name -> wf)
      ResponseData("success", s"成功添加工作流${wf.name}", null)
    } else {
      workflows = workflows + (wf.name -> wf)
      ResponseData("success", s"成功替换工作流${wf.name}", null)
    }
  }
  /**
   * 测试xml合法性
   */
  def checkXml(xmlStr: String): ResponseData = {
    var wf: WorkflowInfo = null
    try {
      wf = WorkflowInfo(xmlStr)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        return ResponseData("fail", "xml解析出错", e.getMessage)
    }
    ResponseData("success", "xml解析成功", null)
  }
  /**
   * 删
   */
  def remove(name: String): Future[ResponseData] = {
    if (!workflows.get(name).isEmpty) {
      val rsF = (Master.persistManager ? Delete(workflows(name))).mapTo[Boolean]
      rsF.map { 
        case x if x == true =>
          LogRecorder.info(WORKFLOW_MANAGER, null, name, s"删除工作流：${name}")
          Master.haDataStorager ! RemoveWorkflow(name)
          workflows = workflows.filterNot { x => x._1 == name }.toMap
          ResponseData("success", s"成功删除工作流${name}", null)
        case _ => 
          ResponseData("fail", s"删除工作流${name}出错(数据库删除失败)", null)
      }
    } else {
      Future{ResponseData("fail", s"工作流${name}不存在", null)}
    }
  }
  def removeInstance(id: String): Future[ResponseData] = {
    if(id == null || id.trim() == ""){
      Future{ResponseData("fail", s"无效实例id", null)}
    }else{
    	val rsF1 = (Master.persistManager ? ExecuteSql(s"delete from workflow_instance where id = '${id}'")).mapTo[Boolean]     
    	val rsF2 = (Master.persistManager ? ExecuteSql(s"delete from node_instance where workflow_instance_id = '${id}'")).mapTo[Boolean]     
			val rsF3 = (Master.persistManager ? ExecuteSql(s"delete from log_record where sid = '${id}'")).mapTo[Boolean]     
      val listF = List(rsF1, rsF2, rsF3)
    	Future.sequence(listF).map {
    	  case l if l.contains(false) =>
    	    ResponseData("fail", s"删除工作流${id}失败（数据库问题）", null)
    	  case _ =>
    	    ResponseData("success", s"成功删除工作流${id}", null)
    	}
    }
  }
  /**
   * 生成工作流实例并执行
   */
  def newAndExecute(wfName: String, paramMap: Map[String, String]): Boolean = {
    if (workflows.get(wfName).isEmpty) {
      LogRecorder.error(WORKFLOW_MANAGER, null, null, s"未找到名称为[${wfName}]的工作流")
      false
    } else {
    	val wfi = WorkflowInstance(workflows(wfName), paramMap)
      //把工作流实例加入到等待队列中
      waittingWorkflowInstance += wfi
      Master.haDataStorager ! AddWWFI(wfi)
      true
    }
  }
  /**
   * 生成工作流实例并执行
   */
  def manualNewAndExecute(wfName: String, paramMap: Map[String, String]): ResponseData = {
    if (workflows.get(wfName).isEmpty) {
      ResponseData("fail", s"工作流${wfName}不存在", null)
    } else {
      val wfi = WorkflowInstance(workflows(wfName), paramMap)
      //把工作流实例加入到等待队列中
      waittingWorkflowInstance += wfi
      Master.haDataStorager ! AddWWFI(wfi)
      ResponseData("success", s"已生成工作流实例,id:${wfi.id}", null)
    }
  }
  /**
   * 工作流实例完成后处理
   */
  def handleWorkFlowInstanceReply(wfInstance: WorkflowInstance): Boolean = {
    //剔除该完成的工作流实例
    val (_, af) = this.workflowActors.get(wfInstance.id).get
    this.workflowActors = this.workflowActors.filterKeys { _ != wfInstance.id }
    Master.haDataStorager ! RemoveRWFI(wfInstance.id)
    Thread.sleep(1000)
    LogRecorder.info(WORKFLOW_MANAGER, wfInstance.id, wfInstance.workflow.name, s"工作流实例：${wfInstance.actorName}执行完毕，执行状态为：${wfInstance.getStatus()}")
    coordinatorManager ! WorkFlowExecuteResult(wfInstance.workflow.name, wfInstance.getStatus())
    //根据状态发送邮件告警
    Thread.sleep(3000)
    if (wfInstance.workflow.mailLevel.contains(wfInstance.getStatus())) {
      val result = htmlMail(wfInstance).map { x => 
        EmailMessage(wfInstance.workflow.mailReceivers,"【Akkaflow告警】",x,List[String]())  
      }
      result pipeTo Master.emailSender
    }
    true
  }
  def htmlMail(wfi: WorkflowInstance): Future[String] = {
    val part1 = s"""
<style> 
  .table-n {text-align: center; border-collapse: collapse;border:1px solid black}
  .table-n th{background-color: #d0d0d0}
  
  /*.warn {width:50px;background-color: orange;margin-right: 5px;border-radius:4px;text-align: center;float: left}
  .error {width:50px;background-color: red;margin-right: 5px;border-radius:4px;text-align: center;float: left}
  .info {width:50px;background-color: #349bfc;margin-right: 5px;border-radius:4px;text-align: center;float: left}*/
  .warn {color:orange;}
  .error {color:red;}
  .info {color:blue;}
  pre {margin:3px;}
  h3 {margin-bottom: 5px}
</style> 
      """
    val part2 = s"""
<h3>实例执行基本信息</h3>
<table>
	<tr><td width="100">实例ID</td><td>${wfi.id}</td></tr>
	<tr><td>工作流</td><td>${wfi.workflow.name}</td></tr>
	<tr><td>运行状态</td><td>${WStatus.getStatusName(wfi.getStatus())}</td></tr>
	<tr><td>开始时间</td><td>${Util.formatStandarTime(wfi.startTime)}</td></tr>
	<tr><td>运行时长</td><td>${((wfi.endTime.getTime-wfi.startTime.getTime)/1000).toInt}</td></tr>
	<tr><td>目录</td><td>${wfi.workflow.dir.dirname}</td></tr>
	<tr><td>参数</td><td>${wfi.paramMap.map{case(k,v) => s"$k:$v"}.mkString(", ")}</td></tr>
	<tr><td>告警级别</td><td>${wfi.workflow.mailLevel.mkString(",")}</td></tr>
	<tr><td>收件人员</td><td>${wfi.workflow.mailReceivers.mkString(",")}</td></tr>
	<tr><td>描述</td><td>${wfi.workflow.desc}</td></tr>
</table>
      """
	import com.kent.workflow.node.NodeInfo.Status
	
	val nodesStr = wfi.nodeInstanceList.map { x => s"""
	  <tr>
	    <td>${x.nodeInfo.name}</td>
	    <td>${x.nodeInfo.getType}</td>
	    <td>${Status.getStatusName(x.status)}</td>
	    <td>${Util.formatStandarTime(x.startTime)}</td>
	    <td>${if(x.startTime == null || x.endTime == null) "--" 
	      else ((x.endTime.getTime-x.startTime.getTime)/1000).toInt}</td>
	    <td>${x.executedMsg}</td>
	    <td>${x.nodeInfo.desc}</td>
	  </tr>
	  """ }.mkString("\n")
	
      val part3 = s"""
<h3>节点执行信息</h3>
<table class="table-n" border="1">
	<tr>
		<th>节点名称</th><th>节点类型</th><th>节点状态</th><th>开始时间</th><th>运行时长</th><th>执行信息</th><th>描述</th>
	</tr>
	${nodesStr}
</table>
        """
	  val logListF = (Master.logRecorder ? GetLog(null,wfi.id, null)).mapTo[List[List[String]]]
	  logListF.map { rows => 
	     val logLines = rows.map { cols => 
	       val aDom = cols(0).toUpperCase() match {
    	      case "INFO" => "<a style='color:blue;margin-right:15px;font-weight:bolder'>INFO    </a>"
    	      case "WARN" => "<a style='color:orange;margin-right:15px;font-weight:bolder'>WARN  </a>"
    	      case "ERROR" => "<a style='color:red;margin-right:15px;font-weight:bolder'>ERROR</a>"
    	      case other => s"<a style='color:black;margin-right:15px;font-weight:bolder'>${other}</a>"
    	    }
	       s"""<pre>${aDom}[${cols(1)}][${cols(2)}] ${cols(3)}</pre>"""
	     }.mkString("\n")
	     val part4 = s"""
	       <h3>执行日志</h3>
	       $logLines
	       """
	     val html = part1 + part2 + part3 + part4
	     html
	  }
  }
  
  
  /**
   * kill掉指定工作流实例
   */
  def killWorkFlowInstance(id: String): Future[ResponseData] = {
    if (!workflowActors.get(id).isEmpty) {
      val wfaRef = workflowActors(id)._2
      val resultF = (wfaRef ? Kill()).mapTo[WorkFlowInstanceExecuteResult]
      this.workflowActors = this.workflowActors.filterKeys { _ != id }.toMap
      Master.haDataStorager ! RemoveRWFI(id)
      val resultF2 = resultF.map {
        case WorkFlowInstanceExecuteResult(x) =>
          ResponseData("success", s"工作流[${id}]已被杀死", x.getStatus())
      }
      resultF2
    } else {
      Future(ResponseData("fail", s"[工作流实例：${id}]不存在，不能kill掉", null))
    }
  }
  /**
   * kill掉指定工作流（包含其所有的运行实例）
   */
  def killWorkFlow(wfName: String): Future[ResponseData] = {
    val result = workflowActors.filter(_._2._1 == wfName).map(x => killWorkFlowInstance(x._1)).toList
    val resultF = Future.sequence(result).map { x =>
      ResponseData("success", s"工作流名称[wfName]的所有实例已经被杀死", null)
    }
    resultF
  }
  /**
   * kill所有工作流
   */
  def killAllWorkFlow(): Future[ResponseData] = {
    val result = workflowActors.map(x => killWorkFlowInstance(x._1)).toList
    val resultF = Future.sequence(result).map { x =>
      ResponseData("success", s"所有工作流实例已经被杀死", null)

    }
    resultF
  }
  
  /**
   * 重跑指定的工作流实例（以最早生成的xml为原型）
   */
  def reRunFormer(wfiId: String): Future[ResponseData] = {
    val wfi = WorkflowInstance(wfiId)
    
    val wfiF = (Master.persistManager ? Get(wfi)).mapTo[Option[WorkflowInstance]]
    wfiF.map { wfiOpt =>
      if (wfiOpt.isEmpty) {
        ResponseData("fail", s"工作流实例[${wfiId}]不存在", null)
      } else {
        if (!workflowActors.get(wfiId).isEmpty) {
          ResponseData("fail", s"工作流实例[${wfiId}]已经在重跑", null)
        } else {
          //重置
          val wfi2 = wfiOpt.get
          wfi2.reset()
          //把工作流实例加入到等待队列中
          if (waittingWorkflowInstance.filter { _.id == wfi2.id }.size >= 1) {
            ResponseData("fail", s"工作流实例[${wfiId}]已经存在等待队列中", null)
          } else {
            waittingWorkflowInstance += wfi2
            Master.haDataStorager ! AddWWFI(wfi)
            ResponseData("success", s"工作流实例[${wfiId}]开始重跑", null)
          }
        }
      }
    }
  }
  /**
   * 重跑指定的工作流实例（以最新生成的xml为原型）
   */
  def reRunNewest(wfiId: String): Future[ResponseData] = {
    val wfi = WorkflowInstance(wfiId)
    val wfiOptF = (Master.persistManager ? Get(wfi)).mapTo[Option[WorkflowInstance]]
    wfiOptF.map{ wfiOpt =>
      if (wfiOpt.isEmpty) {
        ResponseData("fail", s"工作流实例[${wfiId}]不存在", null)
      }else if(!workflowActors.get(wfiId).isEmpty) {
        ResponseData("fail", s"工作流实例[${wfiId}]已经在重跑", null)
      }else if(this.workflows.get(wfiOpt.get.workflow.name).isEmpty){
        ResponseData("fail", s"找不到该工作流实例[${wfiId}]中的工作流[${wfiOpt.get.workflow.name}]", null)
      }else{
        val xmlStr = this.workflows.get(wfiOpt.get.workflow.name).get.xmlStr
        val wfi2 = WorkflowInstance(wfiId, xmlStr, wfiOpt.get.paramMap)
        wfi2.reset()
        //把工作流实例加入到等待队列中
        if (waittingWorkflowInstance.filter { _.id == wfi2.id }.size >= 1) {
          ResponseData("fail", s"工作流实例[${wfiId}]已经存在等待队列中", null)
        }else{
          waittingWorkflowInstance += wfi2
          Master.haDataStorager ! AddWWFI(wfi)
          ResponseData("success", s"工作流实例[${wfiId}]开始重跑", null)
        }
      }
      
    }
  }
  /**
   * 获取等待队列信息
   */
  def getWaittingNodeInfo(): ResponseData = {
    val wns = this.waittingWorkflowInstance.map { x => Map("wfid" -> x.id, "name" -> x.workflow.name) }.toList
    ResponseData("success", "成功获取等待队列信息", wns)
  }

  def readFiles(path: String): FileContent = {
    val f = new File(path)
    //
    val config = context.system.settings.config
    val maxSize = config.getString("akka.remote.netty.tcp.maximum-frame-size").toLong
    if (!f.exists()) {
      FileContent(false, s"文件${path}不存在", path, null)
    } else if (f.length() > maxSize) {
      FileContent(false, s"文件${path}大小为${f.length()},超过消息大小上限${maxSize}", path, null)
    } else {
      try {
        val byts = FileUtil.readFile(f)
        FileContent(true, s"文件读取成功", path, byts)
      } catch {
        case e: Exception =>
          e.printStackTrace()
          FileContent(false, s"读取文件失败：${e.getMessage}", path, null)
      }
    }
  }
  /**
   * receive方法
   */
  def indivivalReceive: Actor.Receive = {
    case Start() => this.start()
    case Stop() =>
      sender ! this.stop(); context.stop(self)
    case AddWorkFlow(content) => sender ! this.add(content, true)
    case CheckWorkFlowXml(xmlStr) => sender ! this.checkXml(xmlStr)
    case RemoveWorkFlow(name) => this.remove(name) pipeTo sender
    case RemoveWorkFlowInstance(id) => this.removeInstance(id) pipeTo sender
    case NewAndExecuteWorkFlowInstance(name, params) => this.newAndExecute(name, params)
    case ManualNewAndExecuteWorkFlowInstance(name, params) => sender ! this.manualNewAndExecute(name, params)
    case WorkFlowInstanceExecuteResult(wfi) => this.handleWorkFlowInstanceReply(wfi)
    case KillWorkFlowInstance(id) => this.killWorkFlowInstance(id) pipeTo sender
    case KllAllWorkFlow() => this.killAllWorkFlow() pipeTo sender
    case KillWorkFlow(wfName) => this.killWorkFlow(wfName) pipeTo sender
    case ReRunWorkflowInstance(wfiId: String, isFormer: Boolean) => 
      val rsF = if(isFormer == true) this.reRunFormer(wfiId) else this.reRunNewest(wfiId)
      rsF pipeTo sender
    case GetManagers(wfm, cm) =>
      coordinatorManager = cm
      context.watch(coordinatorManager)
    case GetWaittingInstances() => sender ! getWaittingNodeInfo()
    //读取文件内容
    case GetFileContent(path)   => sender ! readFiles(path)

    case Terminated(arf)        => if (coordinatorManager == arf) log.warning("coordinatorManager actor挂掉了...")
    case Tick()                 => tick()
  }
}

object WorkFlowManager {
  def apply(wfs: List[WorkflowInfo]): WorkFlowManager = {
    WorkFlowManager(wfs, null)
  }
  def apply(wfs: List[WorkflowInfo], waittingWIFs: List[WorkflowInstance]) = {
    val wfm = new WorkFlowManager;
    if (wfs != null) {
      wfm.workflows = wfs.map { x => x.name -> x }.toMap
    }
    if (waittingWIFs != null) {
      wfm.waittingWorkflowInstance ++= waittingWIFs
    }
    wfm
  }
  def apply(contents: Set[String]): WorkFlowManager = {
    WorkFlowManager(contents.map { WorkflowInfo(_) }.toList)
  }
}