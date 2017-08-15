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
import com.kent.db.LogRecorder.LogType
import com.kent.db.LogRecorder.LogType._
import com.kent.db.LogRecorder
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
  implicit val timeout = Timeout(20 seconds)
  private var executeResult: Process = _

  override def execute(): Boolean = {
    def writeToFile(path: String, content: List[String]){
      val writer = new PrintWriter(new File(path))
        val trimContent = content.mkString("\n")
        writer.flush()
        writer.close()
        LogRecorder.info(ACTION_NODE_INSTANCE, this.id, this.nodeInfo.name, s"拷贝到到文件：${path}")
    }
    
    try {
      this.actionActor.workflowActorRef
      val wfmPath = this.actionActor.workflowActorRef.path.parent
      val wfmRef = this.actionActor.context.actorSelection(wfmPath)
      //获取附件
      val attachFileFl = this.nodeInfo.attachFiles.map { fp => 
        (wfmRef ? GetFileContent(fp)).mapTo[FileContent]
      }.toList
      val attachFileF = Future.sequence(attachFileFl)
      //获取可执行文件
      val executeFilePath = this.nodeInfo.analysisExecuteFile()
      val executeFileF = (wfmRef ? GetFileContent(executeFilePath)).mapTo[FileContent]
      
      val resultF = for{
        ef <- executeFileF
        afs <- attachFileF
      } yield (ef, afs)
      //同步执行
      val (executeFileContent, attachFileContents) = Await.result(resultF, 120 seconds)
      //能读取到文件
      if(!executeFileContent.isSuccessed){
        LogRecorder.error(ACTION_NODE_INSTANCE, this.id, this.nodeInfo.name, executeFileContent.msg)
        false
      }else if(attachFileContents.filter{ ! _.isSuccessed}.size > 0){
        attachFileContents.filter{ ! _.isSuccessed}.foreach { x => 
          LogRecorder.error(ACTION_NODE_INSTANCE, this.id, this.nodeInfo.name, x.msg)
        }
        false
      }else{
        //创建执行目录
        var location = Worker.config.getString("workflow.action.script-location") + "/" + s"action_${this.id}_${this.nodeInfo.name}"
        val dir = new File(location)
        dir.deleteOnExit()
        dir.mkdirs()
        //写入执行文件
        val efn = FileUtil.getFileName(executeFileContent.path)
        writeToFile(s"${location}/${efn}", executeFileContent.content)
        //写入附件文件
        attachFileContents.foreach { x => 
          val afn = FileUtil.getFileName(x.path)
          writeToFile(s"${location}/${afn}", x.content)
        }
        //执行
        
        ???
      }
      
    } catch {
      case e: Exception => e.printStackTrace()
      false
    }
//      var newLocation = if(nodeInfo.location == null || nodeInfo.location == "")
//            Worker.config.getString("workflow.action.script-location") + "/" + Util.produce8UUID
//            else nodeInfo.location
//      //把文件内容写入文件
//      val writer = new PrintWriter(new File(newLocation))
//      //删除前置空格
//      val lines = nodeInfo.content.split("\n").map { x => 
//        if(x.trim() != "")x.trim() else null
//      }.filter { _ != null }
//      val trimContent = lines.mkString("\n")
//      writer.write(trimContent)
//      writer.flush()
//      writer.close()
//      LogRecorder.info(ACTION_NODE_INSTANCE, this.id, this.nodeInfo.name, s"代码写入到文件：${newLocation}")
//      //执行
//      var cmd = "sh";
//      if(lines.size > 0){  //选择用哪个执行解析器
//        if(lines(0).toLowerCase().contains("perl")) cmd = "perl"
//        else if(lines(0).toLowerCase().contains("python")) cmd = "python"
//      }
//      
//      val pLogger = ProcessLogger(line => LogRecorder.info(ACTION_NODE_INSTANCE, this.id, this.nodeInfo.name, line),
//                                  line => LogRecorder.error(ACTION_NODE_INSTANCE, this.id, this.nodeInfo.name, line))
//      executeResult = Process(s"${cmd} ${newLocation}").run(pLogger)
//      
//      if(executeResult.exitValue() == 0) true else false
//    }catch{
//      case e:Exception => 
//        e.printStackTrace();
//        LogRecorder.error(ACTION_NODE_INSTANCE, this.id, this.nodeInfo.name, e.getMessage)
//        false
//    }
  }

  def replaceParam(param: Map[String, String]): Boolean = {
    /*nodeInfo.location = ParamHandler(new Date()).getValue(nodeInfo.location, param)
    nodeInfo.content = ParamHandler(new Date()).getValue(nodeInfo.content, param)*/
    true
  }

  def kill(): Boolean = {
    if(executeResult != null){
      executeResult.destroy()
    }
    true
  }
}

object FileExecutorNodeInstance {
  def apply(san: FileExecutorNode): FileExecutorNodeInstance = new FileExecutorNodeInstance(san)
}