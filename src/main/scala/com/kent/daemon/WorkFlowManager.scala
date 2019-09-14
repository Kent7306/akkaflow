package com.kent.daemon

import akka.actor.{Actor, ActorRef, Cancellable, Props}
import akka.pattern.{ask, pipe}
import com.kent.daemon.HaDataStorager._
import com.kent.daemon.LogRecorder.LogType._
import com.kent.main.Master
import com.kent.pub.Event._
import com.kent.pub._
import com.kent.pub.actor.Daemon
import com.kent.pub.dao.{DirectoryDao, WorkflowDao, WorkflowInstanceDao}
import com.kent.workflow.Coor.TriggerType
import com.kent.workflow.Coor.TriggerType._
import com.kent.workflow.Workflow.WStatus
import com.kent.workflow.Workflow.WStatus._
import com.kent.workflow.node.Node.Status
import com.kent.workflow.node.action.{ActionNode, ActionNodeInstance}
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
    * @return
    */
  def start(): Boolean = {
    //初始化(从数据库中读取工作流)
    val xmls = WorkflowDao.findAllXml()
    val responses = xmls.map { case (xml,filePath) => this.add(xml, filePath) }
    responses.foreach {
      case SucceedResult(msg, _) =>  log.info(s"解析数据库的工作流: ${msg}")
      case FailedResult(msg, _) => log.error(s"解析数据库的工作流: ${msg}")
    }
    infoLog(null, null, s"工作流管理器启动...")
    //重跑上次中断的工作流
    val ids = WorkflowInstanceDao.getPrepareAndRunningWFIds()
    ids.map(reRunFormer).foreach{
      case SucceedResult(msg, _) => log.info(msg)
      case FailedResult(msg, _) => log.error(msg)
    }
    //扫描
    this.scheduler = context.system.scheduler.schedule(2000 millis, 600 millis) {
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
      //实例上限
      for (wfi <- waittingWorkflowInstance) {
        val runningInstanceNum = workflowActors.map { case (_, (instance, _)) => instance }
          .count(_.workflow.name == wfi.workflow.name)
        if (runningInstanceNum < wfi.workflow.instanceLimit) {
          waittingWorkflowInstance -= wfi
          return Some(wfi)
        }
      }
      None
    }
    //从工作流集合中找到满足触发条件的工作流
    workflows.foreach { case(name,wf) => 
      if (wf.coorOpt.isDefined && wf.coorOpt.get.isSatisfyTrigger()) {
        this.addToWaittingInstances(wf.name, wf.coorOpt.get.translateParam(), None, TriggerType.NEXT_SET)
        wf.resetCoor()
      }
    }
    //开始运行实例
    val wfiOpt = getSatisfiedWFIfromWaitingWFIs()
    if (wfiOpt.isDefined) {
      val wfi = wfiOpt.get
      val wfActorRef = context.actorOf(Props(WorkflowActor(wfi)), wfi.actorName)
      infoLog(wfi.id, wfi.workflow.name, s"生成工作流实例：${wfi.actorName}")
      workflowActors = workflowActors + (wfi.id -> (wfi, wfActorRef))
      WorkflowInstanceDao.update(wfi)
      Master.haDataStorager ! AddRWFI(wfi)
      wfActorRef ! Start()
    }
  }

  override def postStop(): Unit = {
    if (scheduler != null && !scheduler.isCancelled) scheduler.cancel()
    super.postStop()
  }

  /**
    * 增
    * @param xmlStr
    * @param path
    * @return
    */
  def add(xmlStr: String, path: String): Result = {
    Try {
      val wf = Workflow(xmlStr)
      wf.filePath = path
      wf.nodeList.foreach { x => x.checkIntegrity(wf) }
      val infoLine = if (workflows.get(wf.name).isEmpty) {
        s"成功添加工作流${wf.name}"
      } else {
        //延续旧工作流的前置依赖状态
        val oldWf = workflows(wf.name)
        if (wf.coorOpt.isDefined && oldWf.coorOpt.isDefined){
          val oldCoor = oldWf.coorOpt.get
          wf.coorOpt.get.depends.foreach{ dep =>
            val oldDepOpt = oldCoor.depends.find(_.workFlowName == dep.workFlowName)
            dep.isReady = if(oldDepOpt.isDefined) oldDepOpt.get.isReady else dep.isReady
          }
        }
        s"成功更新工作流${wf.name}"
      }
      workflows = workflows + (wf.name -> wf)
      infoLog(null, wf.name, infoLine)
      WorkflowDao.merge(wf)
      Master.haDataStorager ! AddWorkflow(wf.deepClone())

      SucceedResult(infoLine, Some(wf.name))
    }.recover{
      case e: Exception =>
        e.printStackTrace()
        FailedResult(s"出错：${e.getMessage}")
    }.get
  }

  /**
    * 测试xml合法性
    * @param xmlStr
    * @return
    */
  def checkXml(xmlStr: String): Result = {
    Try {
      val wf = Workflow(xmlStr)
      val result = wf.checkIfAllDependExists(workflows.values.toList)
      if(result.isFail) throw new Exception(s"""不存在前置工作流: ${result.data[List[String]].mkString(",")}""")
      val checkRingRs = wf.checkDependDAG(workflows.values.toList)
      if (checkRingRs.isFail){
        throw new Exception(s"${checkRingRs.message}, 分别为：${checkRingRs.data[List[String]].mkString(",")}")
      }
      val existMsg = if (workflows.get(wf.name).isDefined) "，工作流已存在" else ""
      SucceedResult("xml解析成功" + existMsg)
    }.recover{
      case e: Exception => FailedResult(s"出错信息: ${e.getMessage}")
    }.get
  }

  /**
    * 删
    * @param name
    * @return
    */
  def remove(name: String): Result = {
    Try {
      if (workflows.get(name).isEmpty) {
        FailedResult(s"工作流${name}不存在")
      } else {
        val depWfs = workflows(name).getTriggerWfs(workflows.values.toList)
        if (depWfs.size > 0) {
          FailedResult(s"该工作流被其他工作流所依赖: ${depWfs.map(_.name).mkString(",")}")
        } else if(workflowActors.exists{case (_, (wfi, _)) => wfi.workflow.name == name}){
          FailedResult(s"该工作流实例正在运行中")
        } else if(waittingWorkflowInstance.exists(_.workflow.name == name)){
          FailedResult(s"该工作流有实例处于就绪中")
        } else {
          //数据库删除
          WorkflowDao.delete(name)
          LogRecorder.info(WORKFLOW_MANAGER, null, name, s"删除工作流：${name}")
          Master.haDataStorager ! RemoveWorkflow(name)
          workflows = workflows.filterNot { x => x._1 == name }
          SucceedResult(s"成功删除工作流${name}")
        }
      }
    }.recover{
      case e: Exception => FailedResult(s"出错信息：${e.getMessage}")
    }.get
  }

  /**
    * 删除空目录
    * @param id
    * @return
    */
  def delWorklowDir(id: Int): Result = {
    Try {
      val isSucceed = DirectoryDao.delDir(id)
      if (isSucceed) {
        SucceedResult(s"删除成功")
      } else {
        FailedResult(s"删除失败，该目录下不能有工作流")
      }
    }.recover{
      case e: Exception => FailedResult(s"出错信息：${e.getMessage}")
    }.get
  }

  /**
    * 删除实例
    * @param id
    * @return
    */
  def removeInstance(id: String): Result = {
    Try {
      if (id == null || id.trim() == "") {
        FailedResult(s"无效实例id")
      } else if (waittingWorkflowInstance.exists(_.id == id)){
        FailedResult(s"工作流${id}在等待队列，无法删除，请先移除")
      } else if (workflowActors.exists(_._1 == id)){
        FailedResult(s"工作流${id}运行中，无法删除，请先杀死")
      } else {
        WorkflowInstanceDao.delete(id)
        SucceedResult(s"成功删除工作流${id}")
      }
    }.recover{case e: Exception =>
      FailedResult(s"删除工作流${id}失败，${e.getMessage}")
    }.get
  }

  /**
    * 生成工作流实例并添加到等待队列中
    * 返回生成的实例ID
    *
    * @param wfName
    * @param paramMap
    * @param ignoreWfNames
    * @param triggerType
    * @return
    */
  private def addToWaittingInstances(wfName: String, paramMap: Map[String, String], ignoreWfNames: Option[List[String]], triggerType: TriggerType):Try[String] = {
    Try{
      if (workflows.get(wfName).isEmpty) {
        val errorMsg = s"未找到名称为[${wfName}]的工作流"
        LogRecorder.error(WORKFLOW_MANAGER, null, null, errorMsg)
        throw new Exception(errorMsg)
      } else {
      	val wfi = WorkflowInstance(workflows(wfName), paramMap)
      	wfi.triggerType = triggerType
        //设置节点是否要忽略执行
        if (ignoreWfNames.isDefined){
          wfi.workflow.nodeList.foreach{
            //只能忽略行动节点
            case node: ActionNode =>
              node.isIgnore = false
              ignoreWfNames.get.foreach {
                case igName if igName == node.name => node.isIgnore = true
                case _ =>
              }
            case _ =>
          }
        }
        //把工作流实例加入到等待队列中
        addWaittingWorkflowInstance(wfi)
        wfi.id
      }
    }
  }
  private def addWaittingWorkflowInstance(wfi: WorkflowInstance) = {
    wfi.reset()
    this.waittingWorkflowInstance += wfi
    WorkflowInstanceDao.merge(wfi)
  }

  /**
    * 手动执行某工作流，并带入参数
    * @param wfName
    * @param paramMap
    * @return
    */
  def manualNewAndExecute(wfName: String, paramMap: Map[String, String]): Result = {
    val rsTry = addToWaittingInstances(wfName, paramMap, None, TriggerType.NO_AFFECT)
    rsTry match {
      case Success(id) => SucceedResult(s"已生成工作流实例,id:${id}", Some(id))
      case Failure(e) => FailedResult(e.getMessage)
    }
  }

  /**
    * 工作流实例完成后处理
    * @param wfInstance
    * @return
    */
  def handleWorkFlowInstanceReply(wfInstance: WorkflowInstance): Boolean = {
    //剔除该完成的工作流实例
    val (_, af) = this.workflowActors(wfInstance.id)
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
            val size1 = depends.count { dep => dep.workFlowName == finishedWfName }
            val size2 = depends.count { dep => bloodWaitExecuteWfNames.contains(dep.workFlowName) }
            if(size1 > 0 && size2 == 0) workflows(wfnames) else null
          }.filter { _ != null }.toList
          //添加到等待队列中
          nextWfs.map { wf =>	addToWaittingInstances(wf.name, workflows(wf.name).coorOpt.get.translateParam(), None, TriggerType.BLOOD_EXCUTE) }
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
    * @param curWfName
    * @param wfs
    * @param nextWfs
    * @return
    */
  private def getNextTriggerWfs(curWfName: String, wfs:Map[String, Workflow],nextWfs: scala.collection.mutable.ArrayBuffer[Workflow]):List[Workflow] = {
      wfs.foreach{case (nextWfname, nextWf) =>
        if(nextWf.coorOpt.isDefined && nextWf.coorOpt.get.isEnabled) {
          nextWf.coorOpt.get.depends.foreach {
            case dep if dep.workFlowName == curWfName =>
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
    * @param id
    * @return
    */
  def killWorkFlowInstance(id: String): Future[Result] = {
    val respF = if (!workflowActors.get(id).isEmpty) {
      val (wfi, wfaRef) = workflowActors(id)
      this.workflowActors = this.workflowActors.filterKeys {_ != id }
      val resultF = (wfaRef ? Kill()).mapTo[WorkFlowInstanceExecuteResult]
      Master.haDataStorager ! RemoveRWFI(id)
      resultF.map {
        case WorkFlowInstanceExecuteResult(x) =>
          if (wfi.triggerType == BLOOD_EXCUTE) bloodWaitExecuteWfNames = List()
          SucceedResult(s"工作流[${id}]已被杀死", Some(x.getStatus()))
      }
    } else {
      Future(FailedResult(s"[工作流实例：${id}]不存在，不能kill掉"))
    }

    respF.recover{case e: Exception =>
      FailedResult(s"[工作流实例杀死失败，${e.getMessage}")
    }
  }

  /**
    * kill掉指定工作流（包含其所有的运行实例）
    * @param wfName
    * @return
    */
  def killWorkFlow(wfName: String): Future[Result] = {
    val result = workflowActors.filter(_._2._1.workflow.name == wfName).map(x => killWorkFlowInstance(x._1)).toList
    val resultF = Future.sequence(result).map { x =>
      if (x.exists(_.isFail)) FailedResult(s"其中有实例杀死失败") else SucceedResult(s"工作流名称[${wfName}]的所有实例已经被杀死")
    }
    resultF
  }

  /**
    * kill所有工作流
    * @return
    */
  def killAllWorkFlow(): Future[Result] = {
    val result = workflowActors.map(x => killWorkFlowInstance(x._1)).toList
    val resultF = Future.sequence(result).map { x =>
      if (x.exists(_.isFail)) FailedResult(s"其中有实例杀死失败") else SucceedResult(s"所有工作流实例已经被杀死")
    }
    resultF
  }

  /**
    * 重跑指定的工作流实例
    * @param wfiId
    * @param isFormer true:用当时生成的xml, false: 用最新生成的xml
    * @param isRecover true:忽略成功的节点，false：所有节点重跑
    * @return
    */
  def reRun(wfiId: String, isFormer: Boolean, isRecover: Boolean): Result = {
    Try {
      val formerOpt = WorkflowInstanceDao.getWithId(wfiId)
      if (formerOpt.isEmpty) {
        FailedResult(s"工作流实例[${wfiId}]不存在")
      } else if (workflowActors.get(wfiId).isDefined) {
        FailedResult(s"工作流实例[${wfiId}]已经在重跑")
      } else if (this.workflows.get(formerOpt.get.workflow.name).isEmpty) {
        FailedResult(s"找不到该工作流实例[${wfiId}]中的工作流[${formerOpt.get.workflow.name}]")
      } else if (waittingWorkflowInstance.exists(_.id == wfiId)) {
        FailedResult(s"工作流实例[${wfiId}]已经存在等待队列中")
      } else {
        val former = formerOpt.get
        //得到重跑的工作流对象
        val wfi = if (isFormer) {
          former
        } else {
          val xmlStr = this.workflows(former.workflow.name).xmlStr
          WorkflowInstance(wfiId, xmlStr, former.paramMap)
        }
        //恢复的情况下，已经重跑的节点就忽略执行
        if (isRecover){
          former.nodeInstanceList.foreach{ fni =>
            if(fni.status == Status.SUCCESSED) {
              wfi.nodeInstanceList.find(_.nodeInfo.name == fni.nodeInfo.name).foreach{
                case ni: ActionNodeInstance =>
                    ni.nodeInfo.isIgnore = true
                case _ =>
              }
            }
          }
        }
        //放入等待队列
        addWaittingWorkflowInstance(wfi)
        SucceedResult(s"工作流实例[${wfiId}]放在等待队列，准备开始重跑", Some(wfiId))
      }
    }.recover{
      case e:Exception => FailedResult(s"工作流实例重跑出错, ${e.getMessage}")
    }.get
  }

  /**
    * 重跑指定的工作流实例（以最早生成的xml为原型）
    * @param wfiId
    * @return
    */
  def reRunFormer(wfiId: String): Result = {
    Try {
      val wfiOpt = WorkflowInstanceDao.getWithId(wfiId)

      if (wfiOpt.isEmpty) {
        FailedResult(s"工作流实例[${wfiId}]不存在")
      } else if (workflowActors.get(wfiId).isDefined) {
        FailedResult(s"工作流实例[${wfiId}]已经在重跑")
      } else if (waittingWorkflowInstance.exists(_.id == wfiOpt.get.id)) {
        FailedResult(s"工作流实例[${wfiId}]已经存在等待队列中")
      } else {
        val wfi = wfiOpt.get
        addWaittingWorkflowInstance(wfi)
        SucceedResult(s"工作流实例[${wfiId}]放在等待队列，准备开始重跑", Some(wfiId))
      }
    }.recover{
      case e:Exception => FailedResult(s"工作流实例重跑出错, ${e.getMessage}")
    }.get
  }

  /**
    * 重跑指定的工作流实例（以最新生成的xml为原型）
    * @param wfiId
    * @return
    */
  def reRunNewest(wfiId: String): Result = {
    Try {
      val shortWfiOpt = WorkflowInstanceDao.getWithoutNodes(wfiId)
      if (shortWfiOpt.isEmpty) {
        FailedResult(s"工作流实例[${wfiId}]不存在")
      } else if (workflowActors.get(wfiId).isDefined) {
        FailedResult(s"工作流实例[${wfiId}]已经在重跑")
      } else if (this.workflows.get(shortWfiOpt.get.workflow.name).isEmpty) {
        FailedResult(s"找不到该工作流实例[${wfiId}]中的工作流[${shortWfiOpt.get.workflow.name}]")
      } else if (waittingWorkflowInstance.exists(_.id == wfiId)) {
        FailedResult(s"工作流实例[${wfiId}]已经存在等待队列中")
      } else {
        val xmlStr = this.workflows(shortWfiOpt.get.workflow.name).xmlStr
        val wfi = WorkflowInstance(wfiId, xmlStr, shortWfiOpt.get.paramMap)
        //放入等待队列
        addWaittingWorkflowInstance(wfi)
        SucceedResult(s"工作流实例[${wfiId}]放在等待队列，准备开始重跑", Some(wfiId))
      }
    }.recover{
      case e:Exception => FailedResult(s"工作流实例重跑出错, ${e.getMessage}")
    }.get
  }

  /**
    * （调用）重置指定调度器
    * @param wfName
    * @return
    */
  def resetCoor(wfName: String): Result = {
    Try {
      if (!workflows.get(wfName).isEmpty && workflows(wfName).coorOpt.isDefined) {
        workflows(wfName).resetCoor()
        SucceedResult(s"成功重置工作流[${wfName}]的调度器状态")
      } else if (!workflows.get(wfName).isEmpty && workflows(wfName).coorOpt.isEmpty) {
        FailedResult(s"工作流[${wfName}]未配置调度")
      } else {
        FailedResult(s"工作流[${wfName}]不存在")
      }
    }.recover{ case e: Exception =>
      FailedResult(s"工作流[${wfName}]重置失败，${e.getMessage}")
    }.get
  }

  /**
    * （调用）触发指定工作流的调度器
    * @param wfName
    * @param ignoreNodeNames
    * @param triggerType
    * @return
    */
  def trigger(wfName: String, ignoreNodeNames: Option[List[String]], triggerType: TriggerType):Result = {
    Try {
      if (workflows.get(wfName).isDefined && workflows(wfName).coorOpt.isDefined) {
        triggerType match {
          case NEXT_SET =>
            val idTry = addToWaittingInstances(wfName, workflows(wfName).coorOpt.get.translateParam(), ignoreNodeNames, triggerType)
            workflows(wfName).resetCoor()
            SucceedResult(s"成功触发工作流[${wfName}: ${idTry.get}]执行", Some(idTry.get))
          case BLOOD_EXCUTE =>
            if (bloodWaitExecuteWfNames.size == 0) {
              val nextRelateWfs = getNextTriggerWfs(wfName, this.workflows, scala.collection.mutable.ArrayBuffer[Workflow]())
              bloodWaitExecuteWfNames = nextRelateWfs.map { wf => wf.name }.toList
              val nextBloodwfNames = nextRelateWfs.map {
                _.name
              }.toList
              val idTry = addToWaittingInstances(wfName, workflows(wfName).coorOpt.get.translateParam(), None, triggerType)
              SucceedResult(s"成功触发工作流[${wfName}: ${idTry.get}]执行,后续依次关联的工作流: ${nextBloodwfNames.mkString(",")}", Some(idTry.get))
            } else {
              FailedResult(s"当前blood触发正在执行，剩余有: ${bloodWaitExecuteWfNames.mkString(",")}")
            }
          case _ =>
            FailedResult(s"工作流[${wfName}]触发类型出错")
        }
      } else if (!workflows.get(wfName).isEmpty && workflows(wfName).coorOpt.isEmpty) {
        FailedResult(s"工作流[${wfName}]未配置调度")
      } else {
        FailedResult(s"工作流[${wfName}]不存在")
      }
    }.recover{ case e: Exception =>
      FailedResult(s"工作流[${wfName}]触发失败，${e.getMessage}")
    }.get
  }

  /**
    * 重置所有工作流状态
    * @return
    */
  def resetAllWorkflow(): Result = {
    this.workflows.foreach{case (name, wf) => wf.resetCoor()}
    SucceedResult(s"成功重置所有工作流状态")
  }

  /**
    * 获取今天剩余触发的任务个数
    * @return
    */
  def getTodayAllLeftTriggerCnt(): Result = {
    val wfCntMap = scala.collection.mutable.HashMap[String, Int]()
    val wfs = workflows.values.toList
    val data = wfs.map{ wf =>
      (wf.name,wf.calculateTriggerCnt(wfCntMap, wfs))
    }.filter{ case (name,cnt) =>
        cnt > 0
    }.toMap
    WorkflowDao.mergePlan(data)
    SucceedResult(s"成功获取数据", Some(data))
  }

  /**
    * receive方法
    * @return
    */
  def individualReceive: Actor.Receive = {
    case Start() => this.start()
    case AddWorkFlow(xmlStr, path) => sender ! this.add(xmlStr, path)
    case CheckWorkFlowXml(xmlStr) => sender ! this.checkXml(xmlStr)
    case RemoveWorkFlow(name) => sender ! this.remove(name)
    case RemoveWorkFlowInstance(id) => sender ! this.removeInstance(id)
    case ManualNewAndExecuteWorkFlowInstance(name, params) => sender ! this.manualNewAndExecute(name, params)
    case WorkFlowInstanceExecuteResult(wfi) => this.handleWorkFlowInstanceReply(wfi)
    case KillWorkFlowInstance(id) => this.killWorkFlowInstance(id) pipeTo sender
    case KllAllWorkFlow() => this.killAllWorkFlow() pipeTo sender
    case KillWorkFlow(wfName) => this.killWorkFlow(wfName) pipeTo sender
    case ReRunWorkflowInstance(wfiId, isFormer, isRecover) =>
      //val response = if(isFormer) this.reRunFormer(wfiId) else this.reRunNewest(wfiId)
      sender ! this.reRun(wfiId, isFormer, isRecover)
    //调度器操作
    case Reset(wfName) => sender ! this.resetCoor(wfName)
    case Trigger(wfName, ignoreNodeNames,triggerType) => sender ! this.trigger(wfName, ignoreNodeNames, triggerType)
    case ResetAllWorkflow() => sender ! this.resetAllWorkflow()
    case GetTodayLeftTriggerCnt(wfName) =>
    case GetTodayAllLeftTriggerCnt() => sender ! getTodayAllLeftTriggerCnt()
    case DelWorklowDir(id) => sender ! delWorklowDir(id)
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