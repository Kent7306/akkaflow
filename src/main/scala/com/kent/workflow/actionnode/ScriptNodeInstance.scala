package com.kent.workflow.actionnode

import com.kent.workflow.node.ActionNodeInstance
import com.kent.workflow.node.NodeInstance
import scala.sys.process.ProcessLogger
import com.kent.main.Worker
import com.kent.pub.Event._
import com.kent.coordinate.ParamHandler
import scala.sys.process._
import java.util.Date
import java.io.PrintWriter
import java.io.File
import com.kent.util.Util
import com.kent.db.LogRecorder.LogType
import com.kent.db.LogRecorder.LogType._
import com.kent.db.LogRecorder
import akka.pattern.{ ask, pipe }
import scala.concurrent.ExecutionContext.Implicits.global
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.concurrent.Await
import com.kent.util.FileUtil

class ScriptNodeInstance(override val nodeInfo: ScriptNode) extends ActionNodeInstance(nodeInfo)  {
  private var executeResult: Process = _
  implicit val timeout = Timeout(60 seconds)
  
  override def execute(): Boolean = {
    try {
      val wfmPath = this.actionActor.workflowActorRef.path.parent
      val wfmRef = this.actionActor.context.actorSelection(wfmPath)
      //获取附件
      val attachFileFl = this.nodeInfo.attachFiles.map { fp => 
        (wfmRef ? GetFileContent(fp)).mapTo[FileContent]
      }.toList 
      val attachFileF = Future.sequence(attachFileFl)
      val attachFileContents = Await.result(attachFileF, 120 seconds)
      //是否能成功读取到文件
      if(attachFileContents.filter{ ! _.isSuccessed}.size > 0){
        attachFileContents.filter{ ! _.isSuccessed}.foreach { x => 
          LogRecorder.error(ACTION_NODE_INSTANCE, this.id, this.nodeInfo.name, x.msg)
        }
        false
      }else{
        //创建执行目录
        var location = Worker.config.getString("workflow.action.script-location") + "/" + s"action_${this.id}_${this.nodeInfo.name}"
        var executeFilePath = s"${location}/run_script"
        val dir = new File(location)
        dir.deleteOnExit()
        dir.mkdirs()
        //写入执行文件
        val lines = nodeInfo.code.split("\n").filter { x => x.trim() != "" }.toList
        FileUtil.writeFile(executeFilePath,lines)
        FileUtil.setExecutable(executeFilePath, true)
        LogRecorder.info(ACTION_NODE_INSTANCE, this.id, this.nodeInfo.name, s"写入到文件：${executeFilePath}")
        //写入附件文件
        attachFileContents.foreach { x => 
          val afn = FileUtil.getFileName(x.path)
          FileUtil.writeFile(s"${location}/${afn}", x.content)
          LogRecorder.info(ACTION_NODE_INSTANCE, this.id, this.nodeInfo.name, s"拷贝到文件：${location}/${afn}")
        }
        //执行
        LogRecorder.info(ACTION_NODE_INSTANCE, this.id, this.nodeInfo.name, s"执行命令: ${executeFilePath}")
        val pLogger = ProcessLogger(line => LogRecorder.info(ACTION_NODE_INSTANCE, this.id, this.nodeInfo.name, line),
                                  line => LogRecorder.error(ACTION_NODE_INSTANCE, this.id, this.nodeInfo.name, line))
        executeResult = Process(s"${executeFilePath} ${nodeInfo.paramLine}").run(pLogger)
        if(executeResult.exitValue() == 0) true else false
      }
    }catch{
      case e:Exception => 
        e.printStackTrace();
        LogRecorder.error(ACTION_NODE_INSTANCE, this.id, this.nodeInfo.name, e.getMessage)
        false
    }
  }

  def kill(): Boolean = {
    if(executeResult != null){
      executeResult.destroy()
    }
    true
  }
}

object ScriptNodeInstance {
  def apply(san: ScriptNode): ScriptNodeInstance = new ScriptNodeInstance(san)
}