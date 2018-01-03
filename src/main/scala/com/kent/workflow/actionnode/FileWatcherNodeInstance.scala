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
import com.kent.workflow.actionnode.FileWatcherNodeInstance.DirNotExistException
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import java.net.URI
import org.apache.hadoop.fs.Path

class FileWatcherNodeInstance(override val nodeInfo: FileWatcherNode)  extends ActionNodeInstance(nodeInfo)  {

  def execute(): Boolean = {
    detectFiles()
  }
  /**
   * 检测hdfs文件情况
   */
  private def getHdfsFiles(vFn: String,vDir: String): Map[String, Long] = {
    	val conf = new Configuration()
    	val fs = FileSystem.get(new URI(vDir), conf)
    	if(fs.exists(new Path(vDir))){
    		val status = fs.listStatus(new Path(vDir))
    		status.map { x => (vDir+"/"+x.getPath.getName, x.getLen) }.toMap
    	}else{
    	  throw new DirNotExistException()
    	}
    	
  }
  /**
   * 检测sftp文件情况
   */
  private def getSftpFiles(vFn: String,vDir: String): Map[String, Long] = {
    ???
  }
  /**
   * 检测ftp文件情况
   */
  private def getFtpFiles(vFn: String,vDir: String): Map[String, Long] = {
    ???
  }
  /**
   * 检测本地文件情况
   */
  private def getLocalFiles(vFn: String,vDir: String): Map[String, Long] = {
    val regx = fileNameFuzzyMatch(vFn).r
    val filesize = Util.convertHumen2Byte(nodeInfo.sizeThreshold)
    val dirFile = new File(vDir)
    //目录必须存在
    if(dirFile.isDirectory() && dirFile.exists()) {
      val files = dirFile.listFiles().filter { x => !regx.findFirstIn(x.getName).isEmpty}.toList
      files.map { x => (x.getAbsolutePath, x.length()) }.toMap
    }else{
      throw new DirNotExistException()
    }
  }
  /**
   * 检测文件
   */
  private def detectFiles():Boolean = {
    try{
    	val files = if(nodeInfo.dir.toLowerCase().matches("hdfs:")){
          getHdfsFiles(nodeInfo.filename, nodeInfo.dir)
        }else if(nodeInfo.dir.toLowerCase().matches("sftp:")){
          getSftpFiles(nodeInfo.filename, nodeInfo.dir)
        }else if(nodeInfo.dir.toLowerCase().matches("ftp:")){
          getFtpFiles(nodeInfo.filename, nodeInfo.dir)
        }else {
          getLocalFiles(nodeInfo.filename, nodeInfo.dir)
        }
    	
    	
    	//检测的文件个数要符合规定个数
      if(files.size >= nodeInfo.numThreshold){
        //存在文件大小低于阈值 并且 设置启动告警
        val smallFiles = files.filter{case (vFn, vSize) => vSize < Util.convertHumen2Byte(nodeInfo.sizeThreshold)}
        //存在文件大小低于阈值 并且 设置启动告警
        if(smallFiles.size > 0 && nodeInfo.isWarnMsgEnable){
          var fileCondStr = ""
          val fileLines = smallFiles.map { case (vFn, vSize) => s"文件：${vFn} -- 大小：${Util.convertByte2Humen(vSize)}" }
          fileLines.foreach(y => fileCondStr = "<p>"+fileCondStr+"</p>"+ y)
          //日志记录
          warnLog(s"工作实例【${this.id}】中节点【${nodeInfo.name}】数据检测低于阈值(${nodeInfo.sizeThreshold})")
          warnLog(s"异常文件以下：")
          fileLines.foreach { x => warnLog(x)}
          //默认系统的告警信息更为详细
          val content = if(nodeInfo.warnMessage == null || nodeInfo.warnMessage.trim() == ""){
                          s"""<p>工作实例【${this.id}】中节点【${nodeInfo.name}】数据检测低于阈值(${nodeInfo.sizeThreshold})</p>
                                <p>异常文件以下：</p>
                              ${fileCondStr}"""
                        } else {
                          nodeInfo.warnMessage
                        }
          actionActor.sendMailMsg(null, "【WARN】FileWatcher数据异常", content)
          true
        }else{
          true 
        }
      }else{
        errorLog(s"检测到目录（${nodeInfo.dir}）符合命名要求的文件（${nodeInfo.filename}）个数为${files.size},少于阈值${nodeInfo.numThreshold}, 当前：${files.size}")
        false
      }
    } catch{
      case e: DirNotExistException => 
        errorLog(s"扫描的目录（${nodeInfo.dir}）不存在")
        false
      case e: Exception =>
        e.printStackTrace()
        errorLog(e.getMessage)
        false
    }
  }
  /**
   * 模糊匹配处理
   */
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
}

object FileWatcherNodeInstance {
  class DirNotExistException() extends Exception
}