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
import com.kent.workflow.actionnode.FileMonitorNodeInstance.DirNotExistException
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import java.net.URI
import org.apache.hadoop.fs.Path
import com.kent.util.FileUtil
import scala.concurrent.Await
import akka.util.Timeout
import scala.concurrent.duration._

class FileMonitorNodeInstance(override val nodeInfo: FileMonitorNode)  extends ActionNodeInstance(nodeInfo)  {

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
    	  throw new DirNotExistException(s"${vDir}目录不存在")
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
    val regx = FileUtil.fileNameFuzzyMatch(vFn)
    val filesize = Util.convertHumen2Byte(nodeInfo.sizeThreshold)
    val dirFile = new File(vDir)
    //目录必须存在
    if(dirFile.isDirectory() && dirFile.exists()) {
      val files = dirFile.listFiles().filter { x => !regx.findFirstIn(x.getName).isEmpty}.toList
      files.map { x => (x.getAbsolutePath, x.length()) }.toMap
    }else{
      throw new DirNotExistException(s"${vDir}目录不存在")
    }
  }
  /**
   * 检测文件
   */
  private def detectFiles():Boolean = {
    this.executedMsg = "自定义消息："+nodeInfo.warnMessage
  	var errorInfo = ""
  	val files = if(nodeInfo.dir.toLowerCase().contains("hdfs:")){
        getHdfsFiles(nodeInfo.filename, nodeInfo.dir)
      }else if(nodeInfo.dir.toLowerCase().contains("sftp:")){
        getSftpFiles(nodeInfo.filename, nodeInfo.dir)
      }else if(nodeInfo.dir.toLowerCase().contains("ftp:")){
        getFtpFiles(nodeInfo.filename, nodeInfo.dir)
      }else {
        getLocalFiles(nodeInfo.filename, nodeInfo.dir)
      }
  	
  	//检测的文件个数要符合规定个数
    if(files.size >= nodeInfo.numThreshold){
      val smallFiles = files.filter{case (vFn, vSize) => vSize < Util.convertHumen2Byte(nodeInfo.sizeThreshold)}
      //存在文件大小低于阈值 并且 设置启动告警
      if(smallFiles.size > 0){
    	  var fileCondStr = s"目录（${nodeInfo.dir}）下存在文件大小低于阈值${nodeInfo.sizeThreshold}\n"
        fileCondStr += "异常文件列表\n"
        val fileLinesStr = smallFiles.map { case (vFn, vSize) => s"文件：${vFn} -- 大小：${Util.convertByte2Humen(vSize)}" }.mkString("\n")
        fileCondStr += fileLinesStr + "\n"
        fileCondStr += "自定义信息: "+ this.nodeInfo.warnMessage
        this.executedMsg = fileCondStr
        false
      }else{
       true 
      }
    }else{
      var fileCondStr = s"检测到目录（${nodeInfo.dir}）符合命名要求的文件（${nodeInfo.filename}）个数为${files.size},少于阈值${nodeInfo.numThreshold}\n"
      fileCondStr += "自定义信息: "+ this.nodeInfo.warnMessage
      this.executedMsg = fileCondStr
      false
    }
  }
  /**
   * 该节点被kill执行的方法
   */
  def kill(): Boolean = {
    true
  }
}

object FileMonitorNodeInstance {
  class DirNotExistException(msg: String) extends Exception(msg)
}