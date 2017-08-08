package com.kent.workflow

import akka.actor.ActorLogging
import akka.actor.Actor
import akka.actor.ActorRef
import scala.collection.immutable.Queue
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.Cancellable
import com.kent.workflow.ActionActor._
import com.kent.workflow.node._
import com.kent.workflow.node.NodeInfo.Status._
import com.kent.workflow.WorkflowInfo.WStatus._
import akka.pattern.ask
import akka.actor.Props
import com.kent.util.Util
import scala.concurrent.Future
import akka.util._
import scala.util.control.NonFatal
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import com.kent.db.PersistManager
import scala.util.Random
import com.kent.main.Master
import com.kent.pub.Event._
import scala.util.Success
import com.kent.pub.ActorTool

class WorkflowActor(val workflowInstance: WorkflowInstance) extends ActorTool {
	import com.kent.workflow.WorkflowActor._
  
  var workflowManageAcotrRef:ActorRef = _
  //正在运行的节点actor
	var runningActors: Map[ActorRef, ActionNodeInstance] = Map()
	//节点等待执行队列
	var waitingNodes = Queue[NodeInstance]()
	
	var scheduler:Cancellable = _
  
  
	import akka.actor.SupervisorStrategy._
  override def supervisorStrategy = OneForOneStrategy(){
    case _:Exception => Stop
  }
  /**
   * 启动workflow
   */
  def start(){
	  log.info(s"[workflow:${this.workflowInstance.actorName}开始启动")
	  this.workflowInstance.status = W_RUNNING
	  //节点替换参数
	  this.workflowInstance.nodeInstanceList.foreach { _.replaceParam(workflowInstance.parsedParams) }
	  //保存工作流实例
	   workflowInstance.startTime = Util.nowDate
    Master.persistManager ! Save(workflowInstance.deepClone())
	  
	  //找到开始节点并加入到等待队列
    val sn = workflowInstance.getStartNode()
    if(!sn.isEmpty && sn.get.ifCanExecuted(workflowInstance)){
    	waitingNodes = waitingNodes.enqueue(sn.get)
    	//启动队列
    	this.scheduler = context.system.scheduler.schedule(0 millis, 100 millis){
    		self ! Tick()
    	}
    }else{
      Master.logRecorder ! Error("WorkflowInstance", this.workflowInstance.id, "找不到开始节点")
    }
  }
	/**
	 * 扫描
	 */
  def tick(){
    //s
    if(waitingNodes.size > 0){
    	val(ni, queue) = waitingNodes.dequeue
    	waitingNodes = queue
    	Master.logRecorder ! Info("WorkflowInstance", this.workflowInstance.id, "执行节点："+ni.nodeInfo.name+"， 类型："+ni.getClass)
	    ni.run(this)
    }
    //动作节点执行超时处理
    if(runningActors.size > 0){  
      val timeoutNodes = runningActors.filter{ case (ar,nodeInstance) => nodeInstance.nodeInfo.timeout != -1}
          .filter{case (ar, nodeInstance) => Util.nowTime - nodeInstance.startTime.getTime > nodeInstance.nodeInfo.timeout*1000}
          .map(_._2.nodeInfo.name).toList
      if(timeoutNodes.size > 0){
        Master.logRecorder ! Error("WorkflowInstance", this.workflowInstance.id, "以下动作节点超时：["+timeoutNodes.mkString(",")+"], 杀死当前工作流")
        this.terminateWithKill()
      }
    }
  }
  /**
   * 处理action节点的返回状态????
   */
 private def handleActionResult(ani: ActionNodeInstance, actionSender: ActorRef){
    val ni = runningActors(actionSender)
    runningActors = runningActors.filter(_._1 != actionSender).toMap
    ni.status = sta
    ni.executedMsg = msg
    ni.terminate(this)
    ni.postTerminate()
 }
 /**
  * 处理action节点返回的执行次数
  */
/* private def handleActionRetryTimes(times: Int, actionSender: ActorRef){
   val ni = runningActors(actionSender).asInstanceOf[ActionNodeInstance]
   ni.hasRetryTimes = times
   Master.persistManager ! Save(ni.deepClone())
 }*/
 

	/**
	 * 创建并开始actor节点
	 */
	def createAndStartActionActor(actionNodeInstance: ActionNodeInstance):Boolean = {
	  //获取worker
	  def getWorker():Future[Option[ActorRef]] = {
		  val masterRef = context.actorSelection(context.parent.path.parent)
	    (masterRef ? AskWorker(actionNodeInstance.nodeInfo.host)).mapTo[Option[ActorRef]].map{ 
  	    case x => x
  	  }
	  }
	  
	  
	  val wF = getWorker()
	  wF.map { 
	    case wOpt if wOpt.isDefined =>  
  	    val worker = wOpt.get
  	    actionNodeInstance.allocateHost = worker.path.address.host.get
	      (worker ? CreateAction(actionNodeInstance.deepCloneAs[ActionNodeInstance])).mapTo[ActorRef].map{ af =>
          if(af != null){
  	        runningActors += (af -> actionNodeInstance)
  	        af ! Start()
  	      } 
        }
	    case wOpt if wOpt.isEmpty => 
	      ???
	      
	  }
	  
	  true
	  /*(masterRef ? AskWorker(actionNodeInstance.nodeInfo.host)).mapTo[GetWorker].map{ 
	    //获取worker
	    case GetWorker(worker) => 
	      if(worker == null){
	        Master.logRecorder ! Error("WorkflowInstance", this.workflowInstance.id, s"无法为节点${actionNodeInstance.nodeInfo.name}分配worker")
	        actionNodeInstance.status = FAILED
	        this.terminateWithFail()
	      }else{
	        actionNodeInstance.allocateHost = worker.path.address.host.get
	        (worker ? CreateAction(actionNodeInstance)).mapTo[ActorRef].map{ af =>
	          if(af != null){
    	        runningActors += (af -> actionNodeInstance)
    	        af ! Start()
    	      } 
	        }
	      }
	  }
		true*/
	}
	/**
	 * kill掉所有运行action
	 */
  def killAllRunningAction():Future[List[ActionExecuteResult]] = {
	  val futures = runningActors.map{case (ar, nodeInstance) => {
	    val result = (ar ? Kill()).mapTo[ActionExecuteResult].recover{ case e: Exception => 
	                    ar ! PoisonPill
	                    ActionExecuteResult(FAILED,"节点超时")
	                 }
		  result.map { rs => 
		      val node = runningActors(ar)
		      node.status = rs.status
		      node.endTime = Util.nowDate
		      node.executedMsg = rs.msg
		      rs
		  }
	  }}.toList
	  Future.sequence(futures).andThen {case Success(x) => x}
	}
  /**
	 * 终止当前工作流actor
	 */
	def terminate(){
		scheduler.cancel()
	  runningActors = Map()
	  this.waitingNodes = Queue()
	  this.workflowInstance.endTime = Util.nowDate
	  workflowManageAcotrRef ! WorkFlowInstanceExecuteResult(workflowInstance)
	  //保存工作流实例
	  Master.persistManager ! Save(workflowInstance.deepClone())
	  context.stop(self)
	}
  /**
   * 手动kill，并反馈给发送的actor
   */
  private def kill(sdr: ActorRef) = terminateWith(sdr, W_KILLED, "手动杀死工作流")

  def terminateWith(sdr:ActorRef, status: WStatus, msg: String) = {
    this.workflowInstance.status = status
    this.workflowInstance.endTime = Util.nowDate
    val resultF = status match {
      case W_SUCCESSED => 
        Master.logRecorder ! Info("WorkflowInstance", this.workflowInstance.id, msg)
        Future{true}
      case _ =>
        Master.logRecorder ! Error("WorkflowInstance", this.workflowInstance.id, msg)
        killAllRunningAction().map { l => if(l.filter { case ActionExecuteResult(sta,msg) => sta == FAILED}.size > 0) false else true }
    }
    resultF.map { x => 
      scheduler.cancel()
  	  runningActors = Map()
  	  this.waitingNodes = Queue()
  	  this.workflowInstance.endTime = Util.nowDate
  	  sdr ! WorkFlowInstanceExecuteResult(workflowInstance.deepCloneAs[WorkflowInstance])
  	  //保存工作流实例
  	  Master.persistManager ! Save(workflowInstance.deepClone())
  	  context.stop(self) 
    }
  }
  
  
  def indivivalReceive: Actor.Receive = {
    case Start() => workflowManageAcotrRef = sender;start()
    case Kill() => kill(sender)
    
    //case ActionExecuteRetryTimes(times) => handleActionRetryTimes(times, sender)
    case ActionExecuteResult(ani: ActionNodeInstance) => handleActionResult(ani, sender)
    case EmailMessage(toUsers, subject, htmlText) => 
      val users = if(toUsers == null || toUsers.size == 0) workflowInstance.workflow.mailReceivers else toUsers
      if(users.size > 0){
      	Master.emailSender ! EmailMessage(users, subject, htmlText)      
      }
    case Tick() => tick()
  }
}

object WorkflowActor {
  def apply(workflowInstance: WorkflowInstance): WorkflowActor = new WorkflowActor(workflowInstance)
}