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
import jnr.ffi.annotations.Synchronized
import com.kent.db.LogRecorder.LogType
import com.kent.db.LogRecorder.LogType._
import com.kent.db.LogRecorder
import java.io.File
import com.kent.util.FileUtil

class WorkflowActor(val workflowInstance: WorkflowInstance) extends ActorTool {
	import com.kent.workflow.WorkflowActor._
  
  var workflowManageActorRef:ActorRef = _
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
  def start():Boolean = {
    infoLog(s"工作流实例:[${this.workflowInstance.workflow.name}:${this.workflowInstance.id}]开始启动")
	  log.debug(s"工作流实例:[${this.workflowInstance.workflow.name}:${this.workflowInstance.id}]开始启动")
	  
	  //保存工作流实例
	  workflowInstance.startTime = Util.nowDate
	  this.workflowInstance.changeStatus(W_RUNNING)
	  //找到开始节点并加入到等待队列
    val sn = workflowInstance.getStartNode()
    if(!sn.isEmpty && sn.get.ifCanExecuted(workflowInstance)){
    	waitingNodes = waitingNodes.enqueue(sn.get)
    	//启动队列
    	this.scheduler = context.system.scheduler.schedule(0 millis, 100 millis){
    		self ! Tick() 
    	}
    	true
    }else{  
      errorLog("找不到开始节点")
      false
    }
  }
	/**
	 * 扫描
	 */
  def tick(){
    //扫描等待队列
    if(waitingNodes.size > 0){
    	val(ni, queue) = waitingNodes.dequeue
    	waitingNodes = queue
    	infoLog("执行节点："+ni.nodeInfo.name+"， 类型："+ni.getClass.getName.split("\\.").last)
	    ni.run(this)
    }
    //动作节点执行超时处理
    if(runningActors.size > 0){  
      val timeoutNodes = runningActors.filter{ case (ar,nodeInstance) => 
        nodeInstance.nodeInfo.timeout != -1 && 
        Util.nowTime - nodeInstance.startTime.getTime > nodeInstance.nodeInfo.timeout*1000
      }.map(_._2.nodeInfo.name).toList
      if(timeoutNodes.size > 0){
        errorLog("以下动作节点超时：["+timeoutNodes.mkString(",")+"], 杀死当前工作流")
        this.terminateWith(W_KILLED, "因为节点["+timeoutNodes.mkString(",")+"]超时，主动杀死当前工作流实例")
      }
    }
  }
  /**
   * 处理action节点的返回状态????
   */
 def handleActionResult(sta: Status, msg: String, actionSender: ActorRef){
    val ni = runningActors(actionSender)
    runningActors = runningActors.filter{case (ar, nodeInstance) => ar != actionSender}.toMap
    //若失败重试
    if(sta == FAILED && ni.hasRetryTimes < ni.nodeInfo.retryTimes){
      ni.hasRetryTimes += 1
       warnLog(s"动作节点[${ni.nodeInfo.name}]执行失败，等待${ni.nodeInfo.interval}秒")
       context.system.scheduler.scheduleOnce(ni.nodeInfo.interval second){
      	 ni.reset()
      	 warnLog(s"动作节点[${ni.nodeInfo.name}]执行失败，进行第${ni.hasRetryTimes}次重试")
      	 ni.run(this)
       }
    }else{
  		ni.executedMsg = msg
  		ni.endTime = Util.nowDate
  		ni.changeStatus(sta)
  		ni.terminate(this)
    	ni.postTerminate()
    }
 }
	/**
	 * 创建并开始actor节点
	 */
	def createAndStartActionActor(actionNodeInstance: ActionNodeInstance):Future[Boolean] = {
	  //获取worker
	  def getWorker():Future[Option[ActorRef]] = {
		  val masterRef = context.actorSelection(context.parent.path.parent)
	    (masterRef ? AskWorker(actionNodeInstance.nodeInfo.host)).mapTo[Option[ActorRef]]
	  }
	  val wF = getWorker()
	  wF.map { 
	    case wOpt if wOpt.isDefined =>  
  	    val worker = wOpt.get
  	    actionNodeInstance.allocateHost = worker.path.address.host.get
  	    infoLog(s"节点[${actionNodeInstance.nodeInfo.name}]分配给Worker[${actionNodeInstance.allocateHost}:${worker.path.address.port.get}]")
	      (worker ? CreateAction(actionNodeInstance.deepCloneAs[ActionNodeInstance])).mapTo[ActorRef].map{ af =>
          if(af != null){
  	        runningActors += (af -> actionNodeInstance)
  	        af ! Start()
  	      } 
        }
  	    true
	    case wOpt if wOpt.isEmpty => 
	      errorLog(s"无法为节点${actionNodeInstance.nodeInfo.name}分配worker(${actionNodeInstance.nodeInfo.host})")
	      actionNodeInstance.changeStatus(FAILED)
	      terminateWith(W_FAILED, "因无发分配woker导致工作流实例执行失败")
	      false
	  }
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
	 * kill掉所有运行action
	 */
  private def killAllRunningAction():Future[List[ActionExecuteResult]] = {
	  val futures = runningActors.map{case (ar, nodeInstance) => {
	    val result = (ar ? Kill()).mapTo[ActionExecuteResult].recover{ case e: Exception => 
	                    ar ! PoisonPill
	                    errorLog(s"杀死actor:${ar}超时，将强制杀死")
	                    ActionExecuteResult(FAILED,"节点超时")
	                 }
		  result.map { rs => 
		      val node = runningActors(ar)
		      node.changeStatus(rs.status)
		      node.endTime = Util.nowDate
		      node.executedMsg = rs.msg
		      rs
		  }
	  }}.toList
	  Future.sequence(futures).andThen {case Success(x) => x}
	}
  /**
   * 手动kill，并反馈给发送的actor
   */
  private def kill(sdr: ActorRef) = terminateWith(sdr, W_KILLED, "手动杀死工作流")
  /**
   * 工作流实例以某种状态结束（主动）
   */
  def terminateWith(status: WStatus, msg: String):Unit = terminateWith(workflowManageActorRef, status, msg)
  /**
   * 工作流实例以某种状态结束（被动）
   */
  def terminateWith(sdr:ActorRef, status: WStatus, msg: String){
    val resultF = status match {
      case W_SUCCESSED => 
        infoLog(msg)
        Future{true}
      case _ => 
        errorLog(msg)
        killAllRunningAction().map { l => if(l.filter { case ActionExecuteResult(sta,msg) => sta == FAILED}.size > 0) false else true }
    }
    resultF.map { x => 
      scheduler.cancel()
  	  runningActors = Map()
  	  this.waitingNodes = Queue()
  	  //保存工作流实例
  	  this.workflowInstance.endTime = Util.nowDate
      this.workflowInstance.changeStatus(status)
      log.debug("工作流实例："+workflowInstance.actorName+"执行完毕.执行状态: "+status)
      sdr ! WorkFlowInstanceExecuteResult(workflowInstance.deepClone())
  	  context.stop(self) 
    }
  }
  /**
   * 某节点实例得到下一个节点并加入到等待队列中
   */
  def getNextNodesToWaittingQueue(node: NodeInstance){
    this.synchronized{
    	val nodes = node.getNextNodes(this.workflowInstance)
			nodes.filter { _.ifCanExecuted(this.workflowInstance) }.foreach { x => waitingNodes = waitingNodes.enqueue(x)}
    }
  }
  
  def indivivalReceive: Actor.Receive = {
    case Start() => workflowManageActorRef = sender;start()
    case Kill() => kill(sender)
    case ActionExecuteResult(sta, msg) => handleActionResult(sta, msg, sender)
    case EmailMessage(toUsers, subject, htmlText, attachFiles) => 
      val users = if(toUsers == null || toUsers.size == 0) workflowInstance.workflow.mailReceivers else toUsers
      if(users.size > 0){
      	Master.emailSender ! EmailMessage(users, subject, htmlText, attachFiles)      
      }
    //读取文件内容
    case GetFileContent(path)   => sender ! readFiles(path)
    case GetDBLink(name) => 
      val xmlLoader = context.actorSelection("../../xml-loader")
      xmlLoader.forward(GetDBLink(name))
    case GetInstanceShortInfo() =>
      sender ! InstanceShortInfo(this.workflowInstance.id, this.workflowInstance.workflow.name,  
          this.workflowInstance.workflow.desc, this.workflowInstance.workflow.dir.dirname)
    case Tick() => tick()
  }
  /**
   * INFO日志级别
   */
  private def infoLog(line: String) = LogRecorder.info(WORFLOW_INSTANCE, this.workflowInstance.id,  workflowInstance.workflow.name, line)
  /**
   * ERROR日志级别
   */
  private def errorLog(line: String) = LogRecorder.error(WORFLOW_INSTANCE, this.workflowInstance.id,  workflowInstance.workflow.name, line)
  /**
   * WARN日志级别
   */
  private def warnLog(line: String) = LogRecorder.warn(WORFLOW_INSTANCE, this.workflowInstance.id,  workflowInstance.workflow.name, line)
}

object WorkflowActor {
  def apply(workflowInstance: WorkflowInstance): WorkflowActor = new WorkflowActor(workflowInstance)
}