package com.kent.daemon

import akka.actor.{Actor, ActorRef, Cancellable, Props}
import akka.pattern.{ask, pipe}
import com.kent.daemon.HaDataStorager._
import com.kent.daemon.LogRecorder.LogType._
import com.kent.main.Master
import com.kent.pub.Event._
import com.kent.pub.actor.Daemon
import com.kent.workflow.Coor.TriggerType
import com.kent.workflow.Coor.TriggerType._
import com.kent.workflow.Workflow.WStatus
import com.kent.workflow.Workflow.WStatus._
import com.kent.workflow.{Workflow, WorkflowActor, WorkflowInstance}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

class WorkFlowManager extends Daemon {
  /**
   * 所有的工作流信息
   * [wfName, workflowInfo]
   */
  var workflows: Map[String, Workflow] = Map()
  /**
   * 运行中的工作流实例actor集合
   * Map[WorflowInstance.id, [WorkfowInstance, workflowActorRef]]
   */
  var workflowActors: Map[String, Tuple2[WorkflowInstance, ActorRef]] = Map()
  /**
   * 等待队列
   */
  val waittingWorkflowInstance = scala.collection.mutable.ListBuffer[WorkflowInstance]()
  //调度器
  var scheduler: Cancellable = _
  /**
   * 用于重跑有血源后置触发的所有工作流（为了性能考虑，同一时间下，只能有一个这样的blood触发）
   * 等待重跑的工作流名称列表
   */
  var bloodWaitExecuteWfNames = List[String]()
  
  /**
   * 启动
   */
  def start(): Boolean = {
    //初始化(从数据库中读取工作流xml，同步阻塞方法)
    val rsF = (Master.persistManager ? Query("select xml_str,file_path from workflow")).mapTo[List[List[String]]]
    val listF = rsF.map{ list =>
      list.filter { _.size > 0 }.map { l => this.add(l(0), l(1), true) }.toList
    }
    val list = Await.result(listF, 20 second)
    list.foreach {
      case x if x.result == "success" =>  log.info(s"解析数据库的工作流: ${x.msg}")
      case x if x.result == "error" => log.error(s"解析数据库的工作流: ${x.msg}")
    }
    infoLog(null, null, s"启动扫描...")
    this.scheduler = context.system.scheduler.schedule(2000 millis, 300 millis) {
      self ! Tick()
    }
    true
  }
  /**
   * 扫描等待队列
   */
  def tick() {
    /**
     * 从等待队列中找到满足运行的工作流实例（实例上限）
     */
    def getSatisfiedWFIfromWaitingWFIs(): Option[WorkflowInstance] = {
      for (wfi <- waittingWorkflowInstance) {
        val runningInstanceNum = workflowActors.map { case (_, (instance, _)) => instance }
          .count(_.workflow.name == wfi.workflow.name)
        if (runningInstanceNum < wfi.workflow.instanceLimit) {
          waittingWorkflowInstance -= wfi
          Master.haDataStorager ! RemoveWWFI(wfi.id)
          return Some(wfi)
        }
      }
      None
    }
    //从工作流集合中找到满足触发条件的工作流
    workflows.foreach { case(name,wf) => 
      if (wf.coorOpt.isDefined && wf.coorOpt.get.isSatisfyTrigger()) {
        this.addToWaittingInstances(wf.name, wf.coorOpt.get.translateParam(), TriggerType.NEXT_SET)
        wf.resetCoor()
      }
    }
    
    //开始运行实例
    val wfiOpt = getSatisfiedWFIfromWaitingWFIs()
    if (!wfiOpt.isEmpty) {
      val wfi = wfiOpt.get
      val wfActorRef = context.actorOf(Props(WorkflowActor(wfi)), wfi.actorName)
      infoLog(wfi.id, wfi.workflow.name, s"生成工作流实例：${wfi.actorName}")
      workflowActors = workflowActors + (wfi.id -> (wfi, wfActorRef))
      Master.persistManager ! Save(wfi)
      Master.haDataStorager ! AddRWFI(wfi)
      wfActorRef ! Start()
    }
  }

  def stop(): Boolean = {
    if (scheduler == null || scheduler.isCancelled) true else scheduler.cancel()
  }

  /**
    * 增
    * @param xmlStr
    * @param path
    * @param isSaved
    * @return
    */
  def add(xmlStr: String, path: String, isSaved: Boolean): ResponseData = {
    var wf: Workflow = null
    try {
      wf = Workflow(xmlStr)
      wf.nodeList.foreach{x => x.checkIntegrity(wf)}
      if(!wf.checkDependDAG(workflows.values.toList)) throw new Exception("任务依赖存在回环")
      wf.filePath = path
    } catch {
      case e: Exception =>
        //e.printStackTrace()
        return ResponseData("fail", "xml解析错误", e.getMessage)
    }
    add(wf, isSaved)
  }
  private def add(wf: Workflow, isSaved: Boolean): ResponseData = {
    if (isSaved) {
      (Master.persistManager ? Delete(wf.deepClone())).mapTo[Boolean].map { x => 
        Master.persistManager ! Save(wf.deepClone())        
      }
    }
    Master.haDataStorager ! AddWorkflow(wf.deepClone())

    if (workflows.get(wf.name).isEmpty) {
      	infoLog(null, wf.name, s"新增工作流")
      workflows = workflows + (wf.name -> wf)
      ResponseData("success", s"成功添加工作流${wf.name}", wf.name)
    } else {
      	infoLog(null, wf.name, s"更新工作流")
      workflows = workflows + (wf.name -> wf)
      ResponseData("success", s"成功更新工作流${wf.name}", wf.name)
    }
  }
  /**
   * 测试xml合法性
   */
  def checkXml(xmlStr: String): ResponseData = {
    var wf: Workflow = null
    try {
      wf = Workflow(xmlStr)
      val result = wf.checkIfAllDependExists(workflows.map(_._2).toList)
      if(result.isFailed) throw new Exception(s"""不存在前置工作流: ${result.data.mkString(",")}""")
      if(!wf.checkDependDAG(workflows.values.toList)) throw new Exception("任务依赖存在回环")
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
    if (workflows.get(name).isEmpty) {
    	Future{ResponseData("fail", s"工作流${name}不存在", null)}
    } else {
      val depWfs = workflows(name).getDependedWfs(workflows.map(_._2).toList)
      if(depWfs.size > 0){
        Future{ResponseData("fail", s"该工作流被其他工作流所依赖: ${depWfs.map(_.name).mkString(",")}", null)}
      }else{
    	  val rsF = (Master.persistManager ? Delete(workflows(name).deepClone())).mapTo[Boolean]
    			  rsF.map { 
    			  case x if x == true =>
    			  LogRecorder.info(WORKFLOW_MANAGER, null, name, s"删除工作流：${name}")
    			  Master.haDataStorager ! RemoveWorkflow(name)
    			  workflows = workflows.filterNot { x => x._1 == name }.toMap
    			  ResponseData("success", s"成功删除工作流${name}", null)
    			  case _ => 
    			  ResponseData("fail", s"删除工作流${name}出错(数据库删除失败)", null)
    	  }
      }
    }
  }
  /**
   * 删除实例
   */
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
   * 生成工作流实例并添加到等待队列中
   * 返回生成的实例ID
   */
  private def addToWaittingInstances(wfName: String, paramMap: Map[String, String], triggerType: TriggerType):Try[String] = {
    Try{
      if (workflows.get(wfName).isEmpty) {
        val errorMsg = s"未找到名称为[${wfName}]的工作流"
        LogRecorder.error(WORKFLOW_MANAGER, null, null, errorMsg)
        throw new Exception(errorMsg)
      } else {
      	val wfi = WorkflowInstance(workflows(wfName), paramMap)
      	wfi.triggerType = triggerType
        //把工作流实例加入到等待队列中
        addWaittingWorkflowInstance(wfi)
        Master.haDataStorager ! AddWWFI(wfi)
        wfi.id
      }
    }
  }
  private def addWaittingWorkflowInstance(wfi: WorkflowInstance) = {
    wfi.reset()
    this.waittingWorkflowInstance += wfi
    Master.persistManager ! Save(wfi)
  }
  /**
   * 手动执行某工作流，并带入参数
   */
  def manualNewAndExecute(wfName: String, paramMap: Map[String, String]): ResponseData = {
    val rsTry = addToWaittingInstances(wfName, paramMap, TriggerType.NO_AFFECT)
    rsTry match {
      case Success(id) => ResponseData("success", s"已生成工作流实例,id:${id}", id)
      case Failure(e) => ResponseData("fail", e.getMessage, null)
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
    
    wfInstance.getStatus() match {
      case W_SUCCESSED => 
        infoLog(wfInstance.id, wfInstance.workflow.name, s"工作流实例${wfInstance.id}执行成功")
      case W_FAILED =>
        errorLog(wfInstance.id, wfInstance.workflow.name, s"工作流实例${wfInstance.id}执行失败")
      case W_KILLED =>
        warnLog(wfInstance.id, wfInstance.workflow.name, s"工作流实例${wfInstance.id}被杀死")
      case x => throw new Exception("未识别状态" + x)
    }
    
    if(wfInstance.getStatus() == W_SUCCESSED){
      wfInstance.triggerType match {
        case NEXT_SET => //设置各个任务的前置依赖状态
          workflows.foreach{ case(name, wf) if wf.coorOpt.isDefined =>
            wf.changeDependStatus(wfInstance.workflow.name, true)
          }
        case BLOOD_EXCUTE =>
          val finishedWfName = wfInstance.workflow.name
          //剔除blood列表
          bloodWaitExecuteWfNames =  bloodWaitExecuteWfNames.filter { _ !=  finishedWfName}.toList
          //找到后置的任务,并且该任务不依赖其他bloor的任务。
          val nextWfs = bloodWaitExecuteWfNames.map { wfnames => 
            val depends = workflows(wfnames).coorOpt.get.depends
            val size1 = depends.filter { dep => dep.workFlowName == finishedWfName }.size
            val size2 = depends.filter { dep => bloodWaitExecuteWfNames.contains(dep.workFlowName) }.size
            if(size1 > 0 && size2 == 0) workflows(wfnames) else null
          }.filter { _ != null }.toList
          //添加到等待队列中
          nextWfs.map { wf =>	addToWaittingInstances(wf.name, workflows(wf.name).coorOpt.get.translateParam(), TriggerType.BLOOD_EXCUTE) }
        case NO_AFFECT => //doing nothing
      }
    }else{
      //非成功的情况下，情况blood等待执行队列
      if(wfInstance.triggerType == BLOOD_EXCUTE){
        bloodWaitExecuteWfNames = List[String]()
      }
    }
    //根据状态发送邮件告警
    if (wfInstance.workflow.mailLevel.contains(wfInstance.getStatus())) {
    	Thread.sleep(3000)
    	val nextTriggerWfs = getNextTriggerWfs(wfInstance.workflow.name, this.workflows, scala.collection.mutable.ArrayBuffer[Workflow]())
    	val relateReceivers = (nextTriggerWfs.flatMap { _.mailReceivers } ++ wfInstance.workflow.mailReceivers).distinct
      val result = wfInstance.htmlMail(nextTriggerWfs).map { html => 
          EmailMessage(relateReceivers, s"【Akkaflow】任务执行${WStatus.getStatusName(wfInstance.getStatus())}", html, List[String]())  
      }
    	if(relateReceivers.size > 0)
        result pipeTo Master.emailSender
    }
    true
  }
  /**
   * 得到后置触发并且可用的任务的工作流列表(递归获取)
   */
  private def getNextTriggerWfs(curWfName: String, wfs:Map[String, Workflow],nextWfs: scala.collection.mutable.ArrayBuffer[Workflow]):List[Workflow] = {
      wfs.foreach{case (nextWfname, nextWf) =>
        if(nextWf.coorOpt.isDefined && nextWf.coorOpt.get.isEnabled) {
          nextWf.coorOpt.get.depends.foreach {
            case dep if(dep.workFlowName == curWfName) =>  
              //避免重复
              if(!nextWfs.exists { _.name == nextWfname}){ 
                nextWfs += nextWf
              	getNextTriggerWfs(nextWfname, wfs, nextWfs)
              }
            case _ => 
          }
        }
      }
    nextWfs.toList
  }
  
  /**
   * kill掉指定工作流实例
   */
  def killWorkFlowInstance(id: String): Future[ResponseData] = {
    if (!workflowActors.get(id).isEmpty) {
      val (wfi, wfaRef) = workflowActors(id)
      val resultF = (wfaRef ? Kill()).mapTo[WorkFlowInstanceExecuteResult]
      this.workflowActors = this.workflowActors.filterKeys { _ != id }.toMap
      Master.haDataStorager ! RemoveRWFI(id)
      val resultF2 = resultF.map {
        case WorkFlowInstanceExecuteResult(x) =>
          if(wfi.triggerType == BLOOD_EXCUTE) bloodWaitExecuteWfNames = List()
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
    val result = workflowActors.filter(_._2._1.workflow.name == wfName).map(x => killWorkFlowInstance(x._1)).toList
    val resultF = Future.sequence(result).map { x =>
      ResponseData("success", s"工作流名称[${wfName}]的所有实例已经被杀死", null)
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
    val wfiF = (Master.persistManager ? Get(wfi.deepClone())).mapTo[Option[WorkflowInstance]]
    wfiF.map { wfiOpt =>
      if (wfiOpt.isEmpty) {
        ResponseData("fail", s"工作流实例[${wfiId}]不存在", null)
      }else if(!workflowActors.get(wfiId).isEmpty){
        ResponseData("fail", s"工作流实例[${wfiId}]已经在重跑", null)
      }else if(waittingWorkflowInstance.filter { _.id == wfiOpt.get.id }.size >= 1){
        ResponseData("fail", s"工作流实例[${wfiId}]已经存在等待队列中", null)
      }else {
        //重置
        val wfi2 = wfiOpt.get
        addWaittingWorkflowInstance(wfi2)
        Master.haDataStorager ! AddWWFI(wfi)
        ResponseData("success", s"工作流实例[${wfiId}]放在等待队列，准备开始重跑", wfiId)
      }
    }
  }
  /**
   * 重跑指定的工作流实例（以最新生成的xml为原型）
   */
  def reRunNewest(wfiId: String): Future[ResponseData] = {
    val wfi = WorkflowInstance(wfiId)
    val wfiOptF = (Master.persistManager ? Get(wfi.deepClone())).mapTo[Option[WorkflowInstance]]
    wfiOptF.map{ wfiOpt =>
      if (wfiOpt.isEmpty) {
        ResponseData("fail", s"工作流实例[${wfiId}]不存在", null)
      }else if(workflowActors.get(wfiId).isDefined) {
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
          addWaittingWorkflowInstance(wfi2)
          Master.haDataStorager ! AddWWFI(wfi.deepClone())
          ResponseData("success", s"工作流实例[${wfiId}]开始重跑", wfiId)
        }
      }
    }
  }
  /**
   * （调用）重置指定调度器
   */
  def resetCoor(wfName: String): ResponseData = {
    if(!workflows.get(wfName).isEmpty && workflows(wfName).coorOpt.isDefined){
      workflows(wfName).resetCoor()
      ResponseData("success",s"成功重置工作流[${wfName}]的调度器状态", null)
    }else if(!workflows.get(wfName).isEmpty && workflows(wfName).coorOpt.isEmpty){
      ResponseData("fail",s"工作流[${wfName}]未配置调度", null)
    }else{
      ResponseData("fail",s"工作流[${wfName}]不存在", null)
    }
  }
    /**
   * （调用）触发指定工作流的调度器
   */
  def trigger(wfName: String, triggerType: TriggerType):ResponseData = {
    if(!workflows.get(wfName).isEmpty && workflows(wfName).coorOpt.isDefined){
      triggerType match {
        case NEXT_SET => 
          val idTry = addToWaittingInstances(wfName, workflows(wfName).coorOpt.get.translateParam(), triggerType)
    	    workflows(wfName).resetCoor()
    	    ResponseData("success",s"成功触发工作流[${wfName}: ${idTry.get}]执行", idTry.get)
        case BLOOD_EXCUTE =>
          if(bloodWaitExecuteWfNames.size == 0) {
        	  val nextRelateWfs = getNextTriggerWfs(wfName, this.workflows, scala.collection.mutable.ArrayBuffer[Workflow]())
    			  bloodWaitExecuteWfNames = nextRelateWfs.map { wf => wf.name }.toList
    			  val nextBloodwfNames = nextRelateWfs.map { _.name }.toList
    			  val idTry = addToWaittingInstances(wfName, workflows(wfName).coorOpt.get.translateParam(), triggerType)
    			  ResponseData("success",s"成功触发工作流[${wfName}: ${idTry.get}]执行,后续依次关联的工作流: ${nextBloodwfNames.mkString(",")}", idTry.get)
          }else{
            ResponseData("fail",s"当前blood触发正在执行，剩余有: ${bloodWaitExecuteWfNames.mkString(",")}", null)
          }
        case _ => 
          ResponseData("fail",s"工作流[${wfName}]触发类型出错", null)
      } 
    }else if(!workflows.get(wfName).isEmpty && workflows(wfName).coorOpt.isEmpty){
      ResponseData("fail",s"工作流[${wfName}]未配置调度", null)
    }else{
      ResponseData("fail",s"工作流[${wfName}]不存在", null)
    }
  }
  /**
   * 重置所有工作流状态
   */
  def resetAllWorkflow():ResponseData = {
    this.workflows.foreach{case (name, wf) => wf.resetCoor()}
    ResponseData("success",s"成功重置所有工作流状态", null)
  }

  /**
    * 获取今天剩余触发的任务个数
    * @return
    */
  def getTodayAllLeftTriggerCnt(): ResponseData = {
    val wfCntMap = scala.collection.mutable.HashMap[String, Int]()
    val wfs = workflows.values.toList
    val data = wfs.map{ wf =>
      (wf.name,wf.calculateTriggerCnt(wfCntMap, wfs))
    }.toMap
    ResponseData("success",s"成功获取数据", data)
  }
  /**
   * receive方法
   */
  def individualReceive: Actor.Receive = {
    case Start() => this.start()
    case Stop() => sender ! this.stop(); context.stop(self)
    case AddWorkFlow(xmlStr, path) => sender ! this.add(xmlStr, path, true)
    case CheckWorkFlowXml(xmlStr) => sender ! this.checkXml(xmlStr)
    case RemoveWorkFlow(name) => this.remove(name) pipeTo sender
    case RemoveWorkFlowInstance(id) => this.removeInstance(id) pipeTo sender
    case ManualNewAndExecuteWorkFlowInstance(name, params) => sender ! this.manualNewAndExecute(name, params)
    case WorkFlowInstanceExecuteResult(wfi) => this.handleWorkFlowInstanceReply(wfi)
    case KillWorkFlowInstance(id) => this.killWorkFlowInstance(id) pipeTo sender
    case KllAllWorkFlow() => this.killAllWorkFlow() pipeTo sender
    case KillWorkFlow(wfName) => this.killWorkFlow(wfName) pipeTo sender
    case ReRunWorkflowInstance(wfiId: String, isFormer: Boolean) => 
      val rsF = if(isFormer == true) this.reRunFormer(wfiId) else this.reRunNewest(wfiId)
      rsF pipeTo sender
    //调度器操作
    case Reset(wfName) => sender ! this.resetCoor(wfName)
    case Trigger(wfName, triggerType) => sender ! this.trigger(wfName, triggerType)
    case ResetAllWorkflow() => sender ! this.resetAllWorkflow()
    case GetTodayLeftTriggerCnt(wfName) =>
    case GetTodayAllLeftTriggerCnt() => sender ! getTodayAllLeftTriggerCnt()
    case Tick() => tick()
  }
  
  /**
   * INFO日志级别
   */
  private def infoLog(id: String, wfName: String,line: String) = LogRecorder.info(WORKFLOW_MANAGER, id, wfName, line)
  /**
   * ERROR日志级别
   */
  private def errorLog(id: String, wfName: String,line: String) = LogRecorder.error(WORKFLOW_MANAGER, id, wfName, line)
  /**
   * WARN日志级别
   */
  private def warnLog(id: String, wfName: String,line: String) = LogRecorder.warn(WORKFLOW_MANAGER, id, wfName, line)
}

object WorkFlowManager {
  def apply(wfs: List[Workflow]): WorkFlowManager = {
    WorkFlowManager(wfs, null)
  }
  def apply(wfs: List[Workflow], waittingWIFs: List[WorkflowInstance]) = {
    val wfm = new WorkFlowManager;
    if (wfs != null) {
      wfm.workflows = wfs.map { x => x.name -> x }.toMap
    }
    if (waittingWIFs != null) {
      waittingWIFs.foreach{wfm.addWaittingWorkflowInstance(_)}
    }
    wfm
  }
  def apply(contents: Set[String]): WorkFlowManager = {
    WorkFlowManager(contents.map { Workflow(_) }.toList)
  }
}