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
    sheduler = context.system.scheduler.scheduleOnce(10 millisecond){
      var executedStatus: Status = FAILED
      var msg:String = null
      for(i <- 0 to actionNodeInstance.nodeInfo.retryTimes; if executedStatus == FAILED)
      {
        //汇报执行重试
        workflowActorRef ! ActionExecuteRetryTimes(i)
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
    		
    		//执行失败后，再次执行前间隔
    		if(executedStatus == FAILED && actionNodeInstance.nodeInfo.retryTimes > 0) 
    		  Thread.sleep(actionNodeInstance.nodeInfo.interval * 1000)
      }
      if(context != null) terminate(actionNodeInstance.status, actionNodeInstance.executedMsg)
  }
  }
  /**
   * 结束
   */
  def terminate(status: Status, msg: String){
		workflowActorRef ! ActionExecuteResult(status, msg)    		  
		sheduler.cancel()
		context.stop(self) 
  }
}

object ActionActor{
  case class ActionExecuteResult(status: Status, msg: String)
  case class ActionExecuteRetryTimes(times: Int)
}