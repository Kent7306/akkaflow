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
import com.kent.db.LogRecorder.LogType
import com.kent.db.LogRecorder.LogType._
import com.kent.db.LogRecorder

class ActionActor(actionNodeInstance: ActionNodeInstance) extends ActorTool {
  var workflowActorRef: ActorRef = _
  def indivivalReceive: Actor.Receive = {
    case Start() => start()
    //外界kill
    case Kill() => kill(sender)
    //自行结束
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
      if(actionNodeInstance.getStatus() == KILLED){
        return
      }
      
      actionNodeInstance.changeStatus(executedStatus)
  		actionNodeInstance.executedMsg = if(executedStatus == FAILED) "节点执行失败" else "节点执行成功"
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
    actionNodeInstance.changeStatus(KILLED)
    actionNodeInstance.executedMsg = "手工杀死节点"
    actionNodeInstance.kill();
    terminate(sdr)
  }
  /**
   * 发送邮件
   */
  def sendMailMsg(toUsers: List[String],subject: String,htmlText: String){
    workflowActorRef ! EmailMessage(toUsers, subject, htmlText, List())
  }
  /**
   * 结束
   */
  def terminate(ar:ActorRef){
    //日志记录
		if(actionNodeInstance.getStatus() == SUCCESSED){
		  LogRecorder.info(ACTION_NODE_INSTANCE, actionNodeInstance.id, actionNodeInstance.nodeInfo.name, actionNodeInstance.executedMsg)
		}else {
		  LogRecorder.error(ACTION_NODE_INSTANCE, actionNodeInstance.id, actionNodeInstance.nodeInfo.name, actionNodeInstance.executedMsg)	  
		}
		//结束
    ar ! ActionExecuteResult(actionNodeInstance.getStatus(),actionNodeInstance.executedMsg) 
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