package com.kent.workflow

import akka.actor.ActorLogging
import akka.actor.Actor
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import com.kent.workflow.node.NodeInfo.Status._
import akka.actor.Cancellable
import akka.actor.ActorRef
import com.kent.workflow.node.ActionNodeInstance
import com.kent.main.Worker
import com.kent.pub.Event._

class ActionActor(actionNodeInstance: ActionNodeInstance) extends Actor with ActorLogging {
  var sheduler:Cancellable = null
  var workflowActorRef: ActorRef = _
  var isKilled = false
  def receive: Actor.Receive = {
    case Start() => start()
        
    case Kill() => actionNodeInstance.kill(); isKilled = true; terminate(sender, KILLED, "节点被kill掉了")
  }
  /**
   * 启动
   */
  def start(){
    workflowActorRef = sender
    actionNodeInstance.actionActor = this
    sheduler = context.system.scheduler.scheduleOnce(10 millisecond){
      Worker.logRecorder ! Info("NodeInstance",actionNodeInstance.id,s"开始执行")
      var executedStatus: Status = FAILED
      var msg:String = null
      for(i <- 0 to actionNodeInstance.nodeInfo.retryTimes; if executedStatus == FAILED)
      {
        //汇报执行重试
        if(i > 0 && !isKilled){
          workflowActorRef ! ActionExecuteRetryTimes(i)
          Worker.logRecorder ! Warn("NodeInstance",actionNodeInstance.id,s"第${i}次重试")
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
    		//日志记录
    		if(executedStatus == FAILED && !isKilled){
    		  Worker.logRecorder ! Error("NodeInstance",actionNodeInstance.id,actionNodeInstance.executedMsg) 
    		}else if(executedStatus == SUCCESSED && !isKilled){
    			Worker.logRecorder ! Info("NodeInstance",actionNodeInstance.id,actionNodeInstance.executedMsg)    		  
    		}
    		//执行失败后，再次执行前间隔
    		if(executedStatus == FAILED && actionNodeInstance.nodeInfo.retryTimes > actionNodeInstance.hasRetryTimes && !isKilled) {
    			Worker.logRecorder ! Warn("NodeInstance",actionNodeInstance.id,s"等待${actionNodeInstance.nodeInfo.interval}秒...")
    			for(n <- 0 to actionNodeInstance.nodeInfo.interval; if !isKilled){
    				Thread.sleep(1000)    			  
    			}
    		}
    		//执行kill
    		if(isKilled){
          return
        }
    		
      }
      if(context != null && !isKilled) terminate(workflowActorRef, actionNodeInstance.status, actionNodeInstance.executedMsg)
    }
  }
  /**
   * 发送邮件
   */
  def sendMailMsg(toUsers: List[String],subject: String,htmlText: String){
    workflowActorRef ! EmailMessage(toUsers, subject, htmlText)
  }
  /**
   * 结束
   */
  def terminate(worflowActor: ActorRef, status: Status, msg: String){
		sheduler.cancel()
		worflowActor ! ActionExecuteResult(status, msg) 
		Worker.logRecorder ! Info("NodeInstance",actionNodeInstance.id,s"执行完毕")
		context.stop(self) 
  }
}

object ActionActor{
  def apply(actionNodeInstance: ActionNodeInstance): ActionActor = {
    val cloneNodeInstance = actionNodeInstance.deepClone().asInstanceOf[ActionNodeInstance]
    new ActionActor(cloneNodeInstance)
  }
}