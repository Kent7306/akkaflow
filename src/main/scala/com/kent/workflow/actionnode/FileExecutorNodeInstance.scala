package com.kent.workflow.actionnode

import com.kent.workflow.node.ActionNodeInstance
import com.kent.workflow.node.NodeInstance
import scala.sys.process.ProcessLogger
import com.kent.main.Worker
import com.kent.coordinate.ParamHandler
import scala.sys.process._
import java.util.Date
import java.io.PrintWriter
import java.io.File
import com.kent.util.Util
import akka.pattern.{ ask, pipe }
import scala.concurrent.ExecutionContext.Implicits.global
import akka.util.Timeout
import scala.concurrent.duration._
import akka.actor.ActorRef
import com.kent.pub.Event._
import scala.concurrent.Future
import scala.concurrent.Await
import com.kent.util.FileUtil

class FileExecutorNodeInstance(nodeInfo: FileExecutorNode) extends ActionNodeInstance(nodeInfo)  {
  implicit val timeout = Timeout(60 seconds)
  private var executeResult: Process = _
  private var dir:File = _
  override def execute(): Boolean = {
    try {
      val wfmPath = this.actionActor.workflowActorRef.path.parent
      val wfmRef = this.actionActor.context.actorSelection(wfmPath)
      //获取附件
      val attachFileFl = this.nodeInfo.attachFiles.map { fp => 
        (wfmRef ? GetFileContent(fp)).mapTo[FileContent]
      }.toList 
      val attachFileF = Future.sequence(attachFileFl)
      //获取可执行文件
      val executeFilePath = this.nodeInfo.analysisExecuteFilePath()
      val executeFileF = (wfmRef ? GetFileContent(executeFilePath)).mapTo[FileContent]
      
      val resultF = for{
        ef <- executeFileF
        afs <- attachFileF
      } yield (ef, afs)
      //同步执行
      val (executeFileContent, attachFileContents) = Await.result(resultF, 120 seconds)
      //能读取到文件
      if(!executeFileContent.isSuccessed){
        errorLog(executeFileContent.msg)
        false
      }else if(attachFileContents.filter{ ! _.isSuccessed}.size > 0){
        attachFileContents.filter{ ! _.isSuccessed}.foreach { x => errorLog(x.msg)}
        false
      }else{
        //创建执行目录
        var location = Worker.config.getString("workflow.action.script-location") + "/" + s"action_${this.id}_${this.nodeInfo.name}"
        dir = new File(location)
        dir.deleteOnExit()
        dir.mkdirs()
        //写入执行文件
        val efn = FileUtil.getFileName(executeFileContent.path)
        FileUtil.writeFile(s"${location}/${efn}", executeFileContent.content)
        FileUtil.setExecutable(s"${location}/${efn}", true)
        infoLog(s"拷贝到文件：${location}/${efn}")
        //写入附件文件
        attachFileContents.foreach { x => 
          val afn = FileUtil.getFileName(x.path)
          FileUtil.writeFile(s"${location}/${afn}", x.content)
          infoLog(s"拷贝到文件：${location}/${afn}")
        }
        //替换到临时目录中执行
        val oldExecFilePath = this.nodeInfo.analysisExecuteFilePath()
        val newCommand = this.nodeInfo.command.replace(oldExecFilePath, s"./${efn}")
        
        //写入执行入口文件
        val runFilePath = s"${location}/akkaflow_run"
        val run_code = """
          source /etc/profile
          cd `dirname $0`
          """ + newCommand
        val runLines = run_code.split("\n").filter { x => x.trim() != "" }.map { _.trim() }.toList
        FileUtil.writeFile(runFilePath,runLines)
        FileUtil.setExecutable(runFilePath, true)
        
    		//执行
        //infoLog(s"执行命令: ${newCommand}")
        val pLogger = ProcessLogger(line => infoLog(line), line => errorLog(line))
        executeResult = Process(runFilePath).run(pLogger)
        if(executeResult.exitValue() == 0) true else false
          true
        }
      
    } catch {
      case e: Exception => 
        e.printStackTrace()
        errorLog(e.getMessage)
        false
    } finally {
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