package com.kent.workflow.actionnode

import com.kent.workflow.node.ActionNodeInstance
import java.util.Date
import com.kent.coordinate.ParamHandler
import java.io.File
import com.kent.util.Util
import com.kent.main.Worker
import com.kent.pub.Event._
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor._

class FileWatcherActionNodeInstance(override val nodeInfo: FileWatcherActionNodeInfo)  extends ActionNodeInstance(nodeInfo)  {

  def execute(): Boolean = {
    if(nodeInfo.dir.toLowerCase().matches("hdfs:")){
      detectHdfsFiles()
    }else if(nodeInfo.dir.toLowerCase().matches("sftp:")){
      detectSftpFiles()
    }else if(nodeInfo.dir.toLowerCase().matches("ftp:")){
      detectFtpFiles()
    }else {
      detectLocalFiles()
    }
  }
  /**
   * 检测hdfs文件情况
   */
  private def detectHdfsFiles(): Boolean = {
    ???
  }
  /**
   * 检测sftp文件情况
   */
  private def detectSftpFiles(): Boolean = {
    ???
  }
  /**
   * 检测ftp文件情况
   */
  private def detectFtpFiles(): Boolean = {
    ???
  }
  /**
   * 检测本地文件情况
   */
  private def detectLocalFiles(): Boolean = {
    val pattern = fileNameFuzzyMatch(nodeInfo.filename).r
    val filesize = Util.convertHumen2Byte(nodeInfo.sizeThreshold)
    val file = new File(nodeInfo.dir)
    //目录必须存在
    if(file.isDirectory() && file.exists()) {
      val files = file.listFiles().filter { x => !pattern.findFirstIn(x.getName).isEmpty}.toList
      //检测的文件个数要符合规定个数
      if(files.size >= nodeInfo.numThreshold){
        val smallerFiles = files.filter { _.length() < filesize }.toList
        //存在文件大小低于阈值 并且 设置启动告警
        if(smallerFiles.size > 0 && nodeInfo.isWarnMsgEnable){
          var fileCondStr = ""
          files.map { x => s"<p>文件：${x.getAbsolutePath} -- 大小：${Util.convertByte2Humen(x.length())}</p>" }
               .foreach(y => fileCondStr = fileCondStr + y)
          //默认系统的告警信息更为详细
          val content = if(nodeInfo.warnMessage == null || nodeInfo.warnMessage.trim() == ""){
                          s"""<p>工作实例【${this.id}】中节点【${nodeInfo.name}】数据检测低于阈值(${nodeInfo.sizeThreshold})</p>
                                <p>异常文件以下：</p>
                              ${fileCondStr}"""
                        } else {
                          nodeInfo.warnMessage
                        }
          actionActor.sendMailMsg(null, "【WARN】FileWatcher数据异常", content)
        }
        true
      } else {
        Worker.logRecorder ! Error("NodeInstance",this.id,s"检测到目录（${nodeInfo.dir}）符合命名要求的文件（${nodeInfo.filename}）个数少于阈值： 阈值：${nodeInfo.numThreshold}, 当前：${files.size}")
        false
      }
    } else {
      Worker.logRecorder ! Error("NodeInstance",this.id,s"扫描的目录（${nodeInfo.dir}）不存在")
      false
    }
  }
  
  private def fileNameFuzzyMatch(fileName: String): String = {
    var name = fileName.replaceAll("\\.", "\\\\.")
    name = name.replaceAll("\\*", "(.\\*?)")
    "^"+name+"$"
  }
  /**
   * 该节点被kill执行的方法
   */
  def kill(): Boolean = {
    true
  }

  def replaceParam(param: Map[String, String]): Boolean = {
    nodeInfo.dir = ParamHandler(new Date()).getValue(nodeInfo.dir, param)
    nodeInfo.filename = ParamHandler(new Date()).getValue(nodeInfo.filename, param)
    nodeInfo.warnMessage = ParamHandler(new Date()).getValue(nodeInfo.warnMessage, param)
    true
  }
}

object FileWatcherActionNodeInstance {
  def apply(fwan: FileWatcherActionNodeInfo): FileWatcherActionNodeInstance = new FileWatcherActionNodeInstance(fwan)
}