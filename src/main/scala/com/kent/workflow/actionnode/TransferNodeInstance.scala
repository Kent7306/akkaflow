package com.kent.workflow.actionnode

import com.kent.workflow.node.ActionNodeInstance
import akka.actor.Props
import com.kent.workflow.actionnode.transfer.Source
import com.kent.workflow.actionnode.transfer.Target
import akka.pattern.{ ask, pipe }
import scala.concurrent.duration._
import com.kent.pub.Event.Start
import akka.util.Timeout
import scala.concurrent.Await
import akka.actor.PoisonPill
import akka.actor.ActorRef
import com.kent.main.Worker
import com.kent.db.LogRecorder.LogType
import com.kent.db.LogRecorder.LogType._
import com.kent.util.FileUtil
import java.io.File
import com.kent.db.LogRecorder
import scala.sys.process.ProcessLogger
import scala.sys.process._

class TransferNodeInstance(override val nodeInfo: TransferNode) extends ActionNodeInstance(nodeInfo) {  
  implicit val timeout = Timeout(3600 seconds)
  var sourceRef: ActorRef = _
  var targetRef: ActorRef = _
  private var executeResult: Process = _
  
  def execute(): Boolean = {
    if(nodeInfo.script.isEmpty){
      executeActorTransfer()
    }else{
      executeScript(nodeInfo.script.get)
    }
  }
  /**
   * 使用actor进行导数
   */
  def executeActorTransfer():Boolean = {
    sourceRef = this.actionActor.context.actorOf(Props(new Source(nodeInfo.sourceInfo)),"source")
    targetRef = this.actionActor.context.actorOf(Props(new Target(nodeInfo.targetInfo, this.nodeInfo.name, this.id, sourceRef)),"target")
    try{
      val resultF = (targetRef ? Start()).mapTo[Boolean]
      val result = Await.result(resultF, 3600 seconds)
      result
    }catch{
      case e: Exception => e.printStackTrace();return false
    }
  }
  
  /**
   * 直接执行脚本
   */
  def executeScript(code: String): Boolean = {
    //创建执行目录
    var location = Worker.config.getString("workflow.action.script-location") + "/" + s"action_${this.id}_${this.nodeInfo.name}"
    val executeFilePath = s"${location}/akkaflow_script"
    val dir = new File(location)
    dir.deleteOnExit()
    dir.mkdirs()
    //写入执行入口文件
    val runFilePath = s"${location}/akkaflow_run"
    val run_code = """
      source /etc/profile
      cd `dirname $0`
      ./akkaflow_script """
    val runLines = run_code.split("\n").filter { x => x.trim() != "" }.map { _.trim() }.toList
    FileUtil.writeFile(runFilePath,runLines)
    FileUtil.setExecutable(runFilePath, true)
    
    //写入执行文件  
    val lines = code.split("\n").filter { x => x.trim() != "" }.map { _.trim() }.toList
    FileUtil.writeFile(executeFilePath,lines)
    FileUtil.setExecutable(executeFilePath, true)
    infoLog(s"写入到文件：${executeFilePath}")
    //执行
    val pLogger = ProcessLogger(line => infoLog(line), line => errorLog(line)) 
    executeResult = Process(s"${runFilePath}").run(pLogger)
    if(executeResult.exitValue() == 0) true else false
    
  }
  
  def kill(): Boolean = {
    if(sourceRef != null) sourceRef ! PoisonPill
    if(targetRef != null) targetRef ! PoisonPill
    if(executeResult != null) executeResult.destroy()
    true
  }
}