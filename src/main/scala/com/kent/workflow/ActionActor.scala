package com.kent.workflow

import akka.actor.ActorLogging
import akka.actor.Actor
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import com.kent.workflow.node.NodeInfo.Status._
import akka.pattern.{ ask, pipe }
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
import java.io.File
import com.kent.util.FileUtil
import akka.util.Timeout
import scala.concurrent.Await
import com.kent.pub.db.DBLink
import scala.util.Try

class ActionActor(actionNodeInstance: ActionNodeInstance) extends ActorTool {
  var workflowActorRef: ActorRef = _
  def indivivalReceive: Actor.Receive = {
    case Start() => start()
    //外界kill
    case Kill() => kill(sender)
    //自行结束
    case Termination() => terminate(workflowActorRef)
  }
  /**
   * 开始执行节点
   */
  def start(){
    actionNodeInstance.actionActor = this
    workflowActorRef = sender
    //异步执行
    def asynExcute(){
      //创建临时目录
      val dir = new File(actionNodeInstance.executeDir)
      dir.delete()
      dir.mkdirs()
      var result = false
      try{
        result = actionNodeInstance.execute()
      }catch{
        case e: Exception =>
          val eMsg = if(actionNodeInstance.executedMsg == null) "" else s"\n${actionNodeInstance.executedMsg}"
          actionNodeInstance.executedMsg = s"${e.getMessage}" + eMsg
          result = false
      }
      val executedStatus = if(result) SUCCESSED else FAILED
      //删除临时目录
      FileUtil.deleteDirOrFile(dir)
      
      //这里，如果主动kill掉的话，不会杀死当前执行线程，所以线程还是会继续执行，看status
      if(actionNodeInstance.getStatus() == KILLED) return
      //设置节点实例的执行后状态及执行信息
      actionNodeInstance.status = executedStatus
      actionNodeInstance.executedMsg = executedStatus match {
        case SUCCESSED => "节点执行成功" 
        case FAILED if actionNodeInstance.executedMsg == null => "节点执行失败"
        case _ => actionNodeInstance.executedMsg
      }

    		//发送邮件
    		//重试次数参数>0，并且当前重试次数=设定的阈值，并且执行失败，就发送节点执行失败邮件
      //（即使重试多次，只发送一次）
    		val config = context.system.settings.config
    		val reTryFailAlamMin = Try(config.getInt("workflow.email.node-retry-fail-times")).getOrElse(0)
    		if(actionNodeInstance.nodeInfo.retryTimes > 0  && actionNodeInstance.hasRetryTimes == reTryFailAlamMin && executedStatus == FAILED){
    		  sendNodeRetryMail(false, actionNodeInstance.executedMsg)
    		}else if(actionNodeInstance.hasRetryTimes > reTryFailAlamMin && executedStatus == SUCCESSED) {  //有重试过并且如果执行成功
    		  sendNodeRetryMail(true, actionNodeInstance.executedMsg)
    		}
    		
  		  //线程执行后，节点执行成功或失败
      if(context != null) self ! Termination()
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
   * 发送节点重试邮件
   */
  def sendNodeRetryMail(result: Boolean, msg: String){
    val newMsg = msg.split("\n").map { "<p>" + _ + "</p>" }.mkString("")
    
    val instanceInfoF = (workflowActorRef ? GetInstanceShortInfo()).mapTo[InstanceShortInfo]
    val instanceInfo = Await.result(instanceInfoF, 20 second)
    val resultTmp = if(result) "成功" else "失败"
    val mailHtml = s"""
        <style> 
        .table-n {text-align: center; border-collapse: collapse;border:1px solid black}
        h3 {margin-bottom: 5px}
        a {color:red;font-weight:bold}
        </style> 
        <h3>实例中的&lt;${actionNodeInstance.nodeInfo.label}/&gt;节点执行${resultTmp},内容如下</h3>
        
          <table class="table-n" border="1">
            <tr><td>实例ID(工作流名称)</td><td>${actionNodeInstance.id}(${instanceInfo.name})</td></tr>
            <tr><td>工作流描述</td><td>${instanceInfo.desc}</td></tr>
            <tr><td>节点名称</td><td>${actionNodeInstance.nodeInfo.name}</td></tr>
            <tr><td>告警信息</td><td><a>${newMsg}</a></td></tr>
            <tr><td>总重试次数</td><td>${actionNodeInstance.nodeInfo.retryTimes}</td></tr>
            <tr><td>当前重试次数</td><td>${actionNodeInstance.hasRetryTimes}</td></tr>
            <tr><td>重试间隔</td><td>${actionNodeInstance.nodeInfo.interval}秒</td></tr>
          </table>
          <a>&nbsp;<a>
        """
       val mailTitle = s"【Akkaflow】${actionNodeInstance.nodeInfo.label}节点执行${resultTmp}"
       
       actionNodeInstance.infoLog(s"发送节点执行${resultTmp}通知邮件")
       this.sendMailMsg(null, mailTitle, mailHtml)
  }
  
  /**
   * 发送邮件
   * 如果toUsers = null，则取工作流中配置的收件人列表
   */
  def sendMailMsg(toUsers: List[String],subject: String,htmlText: String){
    workflowActorRef ! EmailMessage(toUsers, subject, htmlText, List())
  }
  /**
   * 获取指定名称的DBLink
   */
  def getDBLink(name: String): Future[Option[DBLink]] = (workflowActorRef ? GetDBLink(name)).mapTo[Option[DBLink]]
  /**
   * 获取工作流实例的简单信息（一般用来发邮件）
   */
  def getInstanceShortInfo(): Future[InstanceShortInfo] = (workflowActorRef ? GetInstanceShortInfo()).mapTo[InstanceShortInfo]
  /**
   * 获取指定文件内容
   */
  def getFileContent(fp: String)(implicit timeout: Timeout): Future[FileContent] = {
    (workflowActorRef ? GetFileContent(fp)).mapTo[FileContent]
  }
  /**
   * 结束
   */
  def terminate(ar:ActorRef){
    //日志记录
		if(actionNodeInstance.getStatus() == SUCCESSED){
		  (s"执行成功："+actionNodeInstance.executedMsg).split("\n").foreach { actionNodeInstance.infoLog(_)}
		}else {
		  (s"执行失败："+actionNodeInstance.executedMsg).split("\n").foreach { actionNodeInstance.errorLog(_)}
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