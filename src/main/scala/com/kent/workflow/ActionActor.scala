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
import scala.util.Success
import scala.concurrent.Future
import com.kent.pub.ActorTool
import com.kent.workflow.node.ActionNodeInstance

class ActionActor(actionNodeInstance: ActionNodeInstance) extends ActorTool {
  var workflowActorRef: ActorRef = _
  def indivivalReceive: Actor.Receive = {
    case Start() => start()
    //外界kill
    case Kill() => kill(sender)
    //
    case Termination() => terminate(workflowActorRef)
  }
  
  def start(){
    actionNodeInstance.actionActor = this
    workflowActorRef = sender
    //异步执行
    def asynExcute(){
      val result = actionNodeInstance.execute()
      val executedStatus = if(result) SUCCESSED else FAILED
      //这里，如果主动kill掉的话，不会杀死进程，所以还是会返回结果，所以是主动杀死的话，看status
      if(actionNodeInstance.status == KILLED){
        return
      }
      
      actionNodeInstance.status = executedStatus
  		actionNodeInstance.executedMsg = if(executedStatus == FAILED) "节点执行失败" else "节点执行成功"
  		//日志记录
  		if(executedStatus == FAILED){
  		  Worker.logRecorder ! Error("NodeInstance",actionNodeInstance.id,actionNodeInstance.executedMsg) 
  		}else if(executedStatus == SUCCESSED){
  			Worker.logRecorder ! Info("NodeInstance",actionNodeInstance.id,actionNodeInstance.executedMsg)    		  
  		}
      if(context != null){
        self ! Termination()
      }
    }
    //起独立线程运行
    val thread = new Thread(new Runnable() {
  		def run() {
  		  	asynExcute()
  		}
	  },s"action_${actionNodeInstance.id}_${actionNodeInstance.name}")
    thread.start()
  }
  def kill(sdr:ActorRef){
    actionNodeInstance.status = KILLED
    actionNodeInstance.executedMsg = "手工杀死节点"
    actionNodeInstance.kill();
    terminate(sdr)
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
  def terminate(ar:ActorRef){
		//结束
    ar ! ActionExecuteResult(actionNodeInstance.status,actionNodeInstance.executedMsg) 
		Worker.logRecorder ! Info("NodeInstance",actionNodeInstance.id,actionNodeInstance.executedMsg)
		context.parent ! RemoveAction(actionNodeInstance.name)
		context.stop(self) 
  }
}

object ActionActor{
  def apply(actionNodeInstance: ActionNodeInstance): ActionActor = {
    val cloneNodeInstance = actionNodeInstance.deepClone().asInstanceOf[ActionNodeInstance]
    new ActionActor(cloneNodeInstance)
  }
}