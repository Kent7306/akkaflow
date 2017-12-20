package com.kent.coordinate

import akka.actor.ActorLogging
import akka.actor.Actor
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.Cancellable
import akka.actor.ActorRef
import com.kent.workflow.WorkflowInfo.WStatus._
import com.kent.main.Master
import com.kent.pub.Event._
import akka.pattern.{ ask, pipe }
import scala.concurrent.duration._
import akka.util.Timeout
import scala.util.Success
import com.kent.ddata.HaDataStorager._
import com.kent.pub.ActorTool
import com.kent.pub.DaemonActor
import com.kent.db.LogRecorder
import com.kent.db.LogRecorder.LogType
import com.kent.db.LogRecorder.LogType._

class CoordinatorManager extends DaemonActor{
  var coordinators: Map[String, Coordinator] = Map()
  var workflowManager: ActorRef = _
  //调度器
  var scheduler: Cancellable = _
  /**
   * 初始化
   */
  //init()
  /**
   * 增
   */
  def add(xmlStr: String, isSaved: Boolean): ResponseData = {
    var coor:Coordinator = null
    try {
    	coor = Coordinator(xmlStr)      
    } catch{
      case e: Exception => e.printStackTrace()
      return ResponseData("fail","content解析错误", null)
    }
    add(coor, isSaved)
  }
  /**
   * 新增coordinator
   */
  private def add(coor: Coordinator, isSaved: Boolean): ResponseData = {
		if(isSaved) Master.persistManager ! Save(coor)
		Master.haDataStorager ! AddCoordinator(coor)
		if(coordinators.get(coor.name).isEmpty){
		  //LogRecorder.info(COORDINATOR, null, coor.name, s"成功添加coordinator：[${coor.name}]")
			coordinators = coordinators + (coor.name -> coor) 
		  ResponseData("success",s"成功添加coordinator[${coor.name}]", null)
		}else{
		  coordinators = coordinators + (coor.name -> coor)
		  LogRecorder.info(COORDINATOR, null, coor.name, s"替换coordinator：[${coor.name}]")
		  ResponseData("success",s"成功替换coordinator[${coor.name}]", null)
		}
  }
  /**
   * 删
   */
  def remove(name: String): ResponseData = {
    if(!coordinators.get(name).isEmpty){
    	Master.persistManager ! Delete(coordinators(name))
    	coordinators = coordinators.filterNot {x => x._1 == name}.toMap
    	LogRecorder.info(COORDINATOR, null, name, s"删除coordinator[${name}]")
    	ResponseData("success",s"成功删除coordinator[${name}]", null)
    }else{
      ResponseData("fail",s"coordinator[${name}]不存在", null)
    }
  }
  /**
   * （调用）重置指定调度器
   */
  def reset(name: String): ResponseData = {
    if(!coordinators.get(name).isEmpty){
      coordinators(name).depends.foreach { x => 
        x.isReady = false
        LogRecorder.info(COORDINATOR, null, name, s"前置依赖工作流[${x.workFlowName}]准备状态设置为：${x.isReady}")
      }
      ResponseData("success",s"成功重置coordinator[${name}]的状态", null)
    }else{
      ResponseData("fail",s"coordinator[${name}]不存在", null)
    }
  }
  /**
   * （调用）触发指定调度器的后置工作流
   */
  def triggerPostWorkflow(name: String):ResponseData = {
    if(!coordinators.get(name).isEmpty){
      coordinators(name).execute(workflowManager, false)
      ResponseData("success",s"成功触发后置工作流[${coordinators(name).workflows.mkString(",")}]", null)
    }else{
      ResponseData("fail",s"coordinator[${name}]不存在", null)
    }
  }
  
  /**
   * 启动
   */
  def start(): Boolean = {
      LogRecorder.info(COORDINATOR, null, null, s"启动扫描...")
      this.scheduler = context.system.scheduler.schedule(0 millis,  200 millis){
        self ! Tick()
      }
    true
  }
  /**
   * 扫描
   */
  def tick() = {
    import com.kent.coordinate.Coordinator.Status._
    coordinators.filter { case(name,coor) => coor.status == ACTIVE }
      .foreach { case(name,coor) => coor.execute(workflowManager) }
  }
  /**
   * 停止
   */
  def stop(): Boolean = {
    LogRecorder.info(COORDINATOR, null, null, s"停止扫描...")
    if(scheduler == null || scheduler.isCancelled) true else scheduler.cancel()
  }
  /**
   * 设置前置依赖的工作流状态
   */
  def setCoordinatorDepend(wfName: String, status: WStatus){
    if(status == W_SUCCESSED)
      coordinators.foreach{ case(name, coor) => coor.changeDependStatus(wfName, true)}
  }
  /**
   * receive方法
   */
  def indivivalReceive: Actor.Receive = {
    case Start() => this.start()
    case Stop() => sender ! this.stop(); context.stop(self)
    case AddCoor(xmlStr) => sender ! this.add(xmlStr, true)
    case RemoveCoor(name) => sender ! this.remove(name)
    case ResetCoor(name) => sender ! this.reset(name)
    case TriggerPostWorkflow(name) => sender ! triggerPostWorkflow(name)
    case WorkFlowExecuteResult(wfName, status) => this.setCoordinatorDepend(wfName, status)
    case GetManagers(wfm, cm) => this.workflowManager = wfm
    case Tick() => tick()
  }
}


object CoordinatorManager{
  def apply(coors: List[Coordinator]):CoordinatorManager = {
    val cm = new CoordinatorManager;
    cm.coordinators = coors.map { x => x.name -> x }.toMap
    cm
  }
  def apply(contents: Set[String]):CoordinatorManager = {
    CoordinatorManager(contents.map { Coordinator(_) }.toList)
  }
}