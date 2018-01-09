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
  var dir:File = null
  
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
        attachFileContents.filter{ ! _.isSuccessed}.foreach { x => errorLog(x.msg) }
        false
      }else{
        //创建执行目录
        var location = Worker.config.getString("workflow.action.script-location") + "/" + s"action_${this.id}_${this.nodeInfo.name}"
        val executeFilePath = s"${location}/akkaflow_script"
        dir = new File(location)
        dir.deleteOnExit()
        dir.mkdirs()
        //写入执行入口文件
        val runFilePath = s"${location}/akkaflow_run"
        val paramLine = if(nodeInfo.paramLine == null) "" else nodeInfo.paramLine
        val run_code = """
          source /etc/profile
          cd `dirname $0`
          ./akkaflow_script """ + paramLine
        val runLines = run_code.split("\n").filter { x => x.trim() != "" }.map { _.trim() }.toList
        FileUtil.writeFile(runFilePath,runLines)
        FileUtil.setExecutable(runFilePath, true)
        
        //写入执行文件  
        val lines = nodeInfo.code.split("\n").filter { x => x.trim() != "" }.map { _.trim() }.toList
        FileUtil.writeFile(executeFilePath,lines)
        FileUtil.setExecutable(executeFilePath, true)
        //infoLog(s"写入到文件：${executeFilePath}")
        //写入附件文件
        attachFileContents.foreach { x => 
          val afn = FileUtil.getFileName(x.path)
          FileUtil.writeFile(s"${location}/${afn}", x.content)
          infoLog(s"拷贝到文件：${location}/${afn}")
        }
        //执行
        //infoLog(s"执行命令: ${executeFilePath} ${paramLine}")
        val pLogger = ProcessLogger(line => infoLog(line), line => errorLog(line))
        executeResult = Process(s"${runFilePath}").run(pLogger)
        if(executeResult.exitValue() == 0) true else false
      }
    }catch{
      case e:Exception => 
        e.printStackTrace();
        errorLog(e.getMessage)
        false
    }finally {
      FileUtil.deleteDirOrFile(dir)
    }
  }

  def kill(): Boolean = {
    if(executeResult != null){
      executeResult.destroy()
    }
    FileUtil.deleteDirOrFile(dir)
    true
  }
}