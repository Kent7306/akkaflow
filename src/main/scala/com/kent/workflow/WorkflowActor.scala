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

class WorkflowActor(val workflowInstance: WorkflowInstance) extends Actor with ActorLogging {
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
    		this.scan()
    	}
    }else{
      Master.logRecorder ! Info("WorkflowInstance", this.workflowInstance.id, "找不到开始节点")
    }
  }
	/**
	 * 扫描
	 */
  def scan(){
    //s
    if(waitingNodes.size > 0){
    	val(ni, queue) = waitingNodes.dequeue
    	waitingNodes = queue
    	Master.logRecorder ! Info("WorkflowInstance", this.workflowInstance.id, "执行节点："+ni.nodeInfo.name+"， 类型："+ni.getClass)
	    ni.run(this)
    }
    //动作节点执行超时处理
    if(runningActors.size > 0){  
      val timeoutNodes = runningActors.filter(_._2.nodeInfo.timeout != -1)
          .filter(x => Util.nowTime - x._2.startTime.getTime > x._2.nodeInfo.timeout*1000)
          .map(_._2.nodeInfo.name).toList
      if(timeoutNodes.size > 0){
        Master.logRecorder ! Error("WorkflowInstance", this.workflowInstance.id, "以下动作节点超时：["+timeoutNodes.mkString(",")+"], 杀死当前工作流")
        this.kill()
      }
    }
  }
  /**
   * 处理action节点的返回状态????
   */
 private def handleActionResult(sta: Status, msg: String, actionSender: ActorRef){
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
 private def handleActionRetryTimes(times: Int, actionSender: ActorRef){
   val ni = runningActors(actionSender).asInstanceOf[ActionNodeInstance]
   ni.hasRetryTimes = times
   Master.persistManager ! Save(ni.deepClone())
 }
 

	/**
	 * 创建并开始actor节点
	 */
	def createAndStartActionActor(actionNodeInstance: ActionNodeInstance):Boolean = {
	  implicit val timeout = Timeout(20 seconds)
	  val masterRef = context.actorSelection(context.parent.path.parent)
	  (masterRef ? AskWorker(actionNodeInstance.nodeInfo.host)).mapTo[GetWorker].map{ 
	    //获取worker
	    case GetWorker(worker) => 
	      if(worker == null){
	        Master.logRecorder ! Error("WorkflowInstance", this.workflowInstance.id, s"无法为节点${actionNodeInstance.nodeInfo.name}分配worker")
	        actionNodeInstance.status = FAILED
	        this.fail()
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
		true
	}
	/**
	 * kill掉所有子actor
	 */
  def killRunningNodeActors(callback: (WorkflowActor,List[ActionExecuteResult]) => Unit){
	  implicit val timeout = Timeout(20 seconds)
	  
	  //kill掉所有运行的actionactor
	  val futures = runningActors.map(x => {
	    val result = ask(x._1, Kill()).mapTo[ActionExecuteResult]
	                .recover{ case e: Exception => 
	                    x._1 ! PoisonPill
	                    ActionExecuteResult(FAILED,"节点超时")
	                 }
		  result.map { y => 
		      val node = runningActors(x._1)
		      node.status = y.status
		      node.endTime = Util.nowDate
		      node.executedMsg = y.msg
		      y 
		  }
	  }).toList
	  val futuresSeq = Future.sequence(futures).andThen {
	    case Success(x) => callback(this, x)
	  }
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
  private def kill(sdr: ActorRef){
    this.workflowInstance.status = W_KILLED
    killRunningNodeActors{ (wfa,aerList) => 
      sdr ! aerList
      wfa.terminate()
    }
  }
  /**
   * 默默kill
   */
  def kill(){
    this.workflowInstance.status = W_KILLED
    killRunningNodeActors{ (wfa,aerList) => 
      wfa.terminate()
    }
  }
  /**
   * 执行失败
   */
  def fail(){
    this.workflowInstance.status = W_FAILED 
    this.killRunningNodeActors{(wfa,aerList) => 
      wfa.terminate()
    }
  }
  
  def receive: Actor.Receive = {
    case Start() => workflowManageAcotrRef = sender;start()
    case Kill() => kill(sender)
    
    case ActionExecuteRetryTimes(times) => handleActionRetryTimes(times, sender)
    case ActionExecuteResult(status, msg) => handleActionResult(status, msg, sender)
    case EmailMessage(toUsers, subject, htmlText) => 
    val users = if(toUsers == null || toUsers.size == 0) workflowInstance.workflow.mailReceivers else toUsers
    if(users.size > 0){
    	Master.emailSender ! EmailMessage(users, subject, htmlText)      
    }
  }
}

object WorkflowActor {
  def apply(workflowInstance: WorkflowInstance): WorkflowActor = new WorkflowActor(workflowInstance)
}