package com.kent.coordinate

import akka.actor.ActorLogging
import akka.actor.Actor
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.Cancellable
import akka.actor.ActorRef
import com.kent.workflow.WorkflowInfo.WStatus._

class CoordinatorManager extends Actor with ActorLogging{
  var coordinators: Map[String, Coordinator] = Map()
  var workflowManager: ActorRef = _
  var persistManager: ActorRef = _
  //调度器
  var scheduler: Cancellable = _
  /**
   * 增
   */
  def add(coor: Coordinator): Boolean = {
    coordinators = coordinators + (coor.name -> coor)
    true
  }
  /**
   * 删
   */
  def remove(name: String): Boolean = {
    coordinators = coordinators.filterNot {x => x._1 == name}.toMap
    true
  }
  /**
   * 改
   */
  def update(coor: Coordinator): Boolean = {
    coordinators = coordinators.map {x => if(x._1 == coor.name) coor.name -> coor else x }.toMap
    true
  }
  /**
   * 初始化
   */
  def init(){
     ??? 
  }
  /**
   * 启动
   */
  def start(): Boolean = {
     this.scheduler = context.system.scheduler.schedule(0 millis, 5 seconds){
      this.scan()
    }
    true
  }
  /**
   * 停止
   */
  def stop(): Boolean = if(scheduler.isCancelled) true else scheduler.cancel()
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
    case AddCoor(content) => this.add(Coordinator(content))
    case RemoveCoor(name) => this.remove(name)
    case UpdateCoor(content) => this.update(Coordinator(content))
    case WorkFlowExecuteResult(wfName, status) => this.setCoordinatorDepend(wfName, status)
    case GetManagers(wfm, cm, pm) => this.workflowManager = wfm; this.persistManager = pm
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
  
  case class GetManagers(workflowManager: ActorRef, coorManager: ActorRef, persistManager: ActorRef)
}