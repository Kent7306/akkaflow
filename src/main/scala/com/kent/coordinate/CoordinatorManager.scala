package com.kent.coordinate

import akka.actor.ActorLogging
import akka.actor.Actor
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.Cancellable
import akka.actor.ActorRef
import com.kent.workflow.WorkflowInfo.WStatus._
import com.kent.pub.ShareData
import com.kent.db.LogRecorder._
import com.kent.db.PersistManager._
import com.kent.main.HttpServer.ResponseData
import akka.pattern.{ ask, pipe }
import scala.concurrent.duration._
import akka.util.Timeout
import scala.util.Success

class CoordinatorManager extends Actor with ActorLogging{
  var coordinators: Map[String, Coordinator] = Map()
  var workflowManager: ActorRef = _
  //调度器
  var scheduler: Cancellable = _
  implicit val timeout = Timeout(20 seconds)
  /**
   * 初始化
   */
  //init()
  /**
   * 增
   */
  def add(content: String, isSaved: Boolean): ResponseData = {
    var coor:Coordinator = null
    try {
    	coor = Coordinator(content)      
    } catch{
      case e: Exception => e.printStackTrace()
      return ResponseData("fail","content解析错误", null)
    }
    add(coor, isSaved)
  }
  /**
   * 新增coordinator
   */
  def add(coor: Coordinator, isSaved: Boolean): ResponseData = {
		if(isSaved) ShareData.persistManager ! Save(coor)
		if(coordinators.get(coor.name).isEmpty){
			ShareData.logRecorder ! Info("CoordinatorManager",null,s"增加coordinator：${coor.name}")
			coordinators = coordinators + (coor.name -> coor) 
		  ResponseData("success",s"成功添加coordinator[${coor.name}]", null)
		}else{
		  coordinators = coordinators + (coor.name -> coor)
		  ShareData.logRecorder ! Info("CoordinatorManager",null,s"替换coordinator：${coor.name}")
		  ResponseData("success",s"成功替换coordinator[${coor.name}]", null)
		}
  }
  /**
   * 删
   */
  def remove(name: String): ResponseData = {
    if(!coordinators.get(name).isEmpty){
    	ShareData.persistManager ! Delete(coordinators(name))
    	coordinators = coordinators.filterNot {x => x._1 == name}.toMap
    	ShareData.logRecorder ! Info("CoordinatorManager",null,s"删除coordinator[${name}]")     
    	ResponseData("success",s"成功删除coordinator[${name}]", null)
    }else{
      ResponseData("fail",s"coordinator[${name}]不存在", null)
    }
  }
  /**
   * 初始化，从数据库中获取coordinators
   */
  def init(){
    import com.kent.pub.ShareData._
    val isEnabled = config.getBoolean("workflow.mysql.is-enabled")
    if(isEnabled){
       val listF = (ShareData.persistManager ? Query("select name from coordinator")).mapTo[List[List[String]]]
       listF.andThen{
         case Success(list) => list.map { x =>
           val coor = new Coordinator(x(0))
           val coorF = (ShareData.persistManager ? Get(coor)).mapTo[Option[Coordinator]]
           coorF.andThen{
             case Success(coorOpt) => 
             if(!coorOpt.isEmpty) add(coorOpt.get, false)
           }
         }
       }
    }
  }
  /**
   * 启动
   */
  def start(): Boolean = {
      ShareData.logRecorder ! Info("CoordinatorManager",null,s"启动扫描...")
      this.scheduler = context.system.scheduler.schedule(0 millis, 5 seconds){
      this.scan()
    }
    true
  }
  /**
   * 停止
   */
  def stop(): Boolean = {
    ShareData.logRecorder ! Info("CoordinatorManager",null,s"停止扫描...")
    if(scheduler.isCancelled) true else scheduler.cancel()
  }
  /**
   * 扫描
   */
  def scan(): Boolean = {
    import com.kent.coordinate.Coordinator.Status._
    coordinators.filter { _._2.status == ACTIVE }.foreach { _._2.execute(workflowManager) }
    return true
  }
  /**
   * 
   */
  def setCoordinatorDepend(wfName: String, status: WStatus){
    if(status == W_SUCCESSED)
      coordinators.foreach(_._2.depends.foreach { x => if(x.workFlowName == wfName)x.isReady=true })
  }
  /**
   * receive方法
   */
  import com.kent.coordinate.CoordinatorManager._
  import com.kent.workflow.WorkFlowManager.WorkFlowExecuteResult
  def receive: Actor.Receive = {
    case Start() => this.start()
    case Stop() => this.stop()
    case AddCoor(content) => sender ! this.add(content, true)
    case RemoveCoor(name) => sender ! this.remove(name)
    case WorkFlowExecuteResult(wfName, status) => this.setCoordinatorDepend(wfName, status)
    case GetManagers(wfm, cm) => this.workflowManager = wfm
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

  
  case class Start()
  case class Stop()
  case class AddCoor(content: String)
  case class RemoveCoor(name: String)
  case class UpdateCoor(content: String)
  
  case class GetManagers(workflowManager: ActorRef, coorManager: ActorRef)
}