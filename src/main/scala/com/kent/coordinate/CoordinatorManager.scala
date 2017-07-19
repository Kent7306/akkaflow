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
		if(isSaved) Master.persistManager ! Save(coor)
		Master.haDataStorager ! AddCoordinator(coor)
		if(coordinators.get(coor.name).isEmpty){
			Master.logRecorder ! Info("CoordinatorManager",null,s"增加coordinator：${coor.name}")
			coordinators = coordinators + (coor.name -> coor) 
		  ResponseData("success",s"成功添加coordinator[${coor.name}]", null)
		}else{
		  coordinators = coordinators + (coor.name -> coor)
		  Master.logRecorder ! Info("CoordinatorManager",null,s"替换coordinator：${coor.name}")
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
    	Master.logRecorder ! Info("CoordinatorManager",null,s"删除coordinator[${name}]")     
    	ResponseData("success",s"成功删除coordinator[${name}]", null)
    }else{
      ResponseData("fail",s"coordinator[${name}]不存在", null)
    }
  }
  /**
   * 初始化，从数据库中获取coordinators
   */
/*  def init(){
    import com.kent.main.Master._
    val isEnabled = config.getBoolean("workflow.mysql.is-enabled")
    if(isEnabled){
       val listF = (Master.persistManager ? Query("select name from coordinator")).mapTo[List[List[String]]]
       listF.andThen{
         case Success(list) => list.map { x =>
           val coor = new Coordinator(x(0))
           val coorF = (Master.persistManager ? Get(coor)).mapTo[Option[Coordinator]]
           coorF.andThen{
             case Success(coorOpt) => 
             if(!coorOpt.isEmpty) add(coorOpt.get, false)
           }
         }
       }
    }
  }*/
  /**
   * 启动
   */
  def start(): Boolean = {
		  import com.kent.coordinate.Coordinator.Status._
      Master.logRecorder ! Info("CoordinatorManager",null,s"启动扫描...")
      this.scheduler = context.system.scheduler.schedule(0 millis,  200 millis){
        coordinators.filter { _._2.status == ACTIVE }.foreach { _._2.execute(workflowManager) }
      }
    true
  }
  /**
   * 停止
   */
  def stop(): Boolean = {
    Master.logRecorder ! Info("CoordinatorManager",null,s"停止扫描...")
    if(scheduler == null || scheduler.isCancelled) true else scheduler.cancel()
  }
  /**
   * 设置前置依赖的工作流状态
   */
  def setCoordinatorDepend(wfName: String, status: WStatus){
    if(status == W_SUCCESSED)
      coordinators.foreach(_._2.depends.foreach { x => if(x.workFlowName == wfName)x.isReady=true })
  }
  /**
   * receive方法
   */
  def receive: Actor.Receive = {
    case Start() => this.start()
    case Stop() => sender ! this.stop(); context.stop(self)
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
}