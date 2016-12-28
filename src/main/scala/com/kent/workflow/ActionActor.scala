package com.kent.workflow

import akka.actor.ActorLogging
import akka.actor.Actor
import scala.concurrent.duration._
import com.kent.workflow.WorkflowActor._
import scala.concurrent.ExecutionContext.Implicits.global
import com.kent.workflow.ActionActor._
import com.kent.workflow.node.NodeInfo.Status._
import akka.actor.Cancellable
import akka.actor.ActorRef
import com.kent.workflow.node.ActionNodeInstance
import com.kent.pub.ShareData
import com.kent.db.LogRecorder._
import com.kent.mail.EmailSender.EmailMessage

class ActionActor(actionNodeInstance: ActionNodeInstance) extends Actor with ActorLogging {
  var sheduler:Cancellable = null
  var workflowActorRef: ActorRef = _
  def receive: Actor.Receive = {
    case Start() => start()
        
    case Kill() => actionNodeInstance.kill();terminate(KILLED, "节点被kill掉了")
  }
  /**
   * 启动
   */
  def start(){
    workflowActorRef = sender
    actionNodeInstance.actionActor = this
    sheduler = context.system.scheduler.scheduleOnce(10 millisecond){
      ShareData.logRecorder ! Info("NodeInstance",actionNodeInstance.id,s"开始执行")
      var executedStatus: Status = FAILED
      var msg:String = null
      for(i <- 0 to actionNodeInstance.nodeInfo.retryTimes; if executedStatus == FAILED)
      {
        //汇报执行重试
        if(i > 0){
          workflowActorRef ! ActionExecuteRetryTimes(i)
          ShareData.logRecorder ! Warn("NodeInstance",actionNodeInstance.id,s"第${i}次重试")
        }
        actionNodeInstance.hasRetryTimes = i
        //开始执行
        var result = false
        try{
        	result = actionNodeInstance.execute()          
        }catch{
          case e: Exception => e.printStackTrace();result = false
        }
    	  executedStatus = if(result) SUCCESSED else FAILED
    	  actionNodeInstance.status = executedStatus
    		actionNodeInstance.executedMsg = if(executedStatus == FAILED) "节点执行失败" else "节点执行成功"
    		if(executedStatus == FAILED){
    		  ShareData.logRecorder ! Error("NodeInstance",actionNodeInstance.id,actionNodeInstance.executedMsg) 
    		}else{
    			ShareData.logRecorder ! Info("NodeInstance",actionNodeInstance.id,actionNodeInstance.executedMsg)    		  
    		}
    		//执行失败后，再次执行前间隔
    		if(executedStatus == FAILED && actionNodeInstance.nodeInfo.retryTimes > 0) 
    		  ShareData.logRecorder ! Warn("NodeInstance",actionNodeInstance.id,s"等待${actionNodeInstance.nodeInfo.interval}秒...")
    		  Thread.sleep(actionNodeInstance.nodeInfo.interval * 1000)
      }
      if(context != null) terminate(actionNodeInstance.status, actionNodeInstance.executedMsg)
    }
  }
  
  def sendMailMsg(toUsers: List[String],subject: String,htmlText: String){
    workflowActorRef ! EmailMessage(toUsers, subject, htmlText)
  }
  /**
   * 结束
   */
  def terminate(status: Status, msg: String){
		workflowActorRef ! ActionExecuteResult(status, msg) 
		ShareData.logRecorder ! Info("NodeInstance",actionNodeInstance.id,s"执行完毕")
		sheduler.cancel()
		context.stop(self) 
  }
}

object ActionActor{
  def apply(actionNodeInstance: ActionNodeInstance): ActionActor = {
    val cloneNodeInstance = actionNodeInstance.deepClone().asInstanceOf[ActionNodeInstance]
    new ActionActor(cloneNodeInstance)
  }
  
  case class ActionExecuteResult(status: Status, msg: String) extends Serializable
  case class ActionExecuteRetryTimes(times: Int) extends Serializable
}