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
    val regx = fileNameFuzzyMatch(vFn).r
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
    var warnContent = ""
  	var errorInfo = ""
  	var flag = true
    
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
        val smallFiles = files.filter{case (vFn, vSize) => vSize < Util.convertHumen2Byte(nodeInfo.sizeThreshold)}
        //存在文件大小低于阈值 并且 设置启动告警
        if(smallFiles.size > 0){
          var fileCondStr = "<p>异常文件列表</p>"
          val fileLines = smallFiles.map { case (vFn, vSize) => s"文件：${vFn} -- 大小：${Util.convertByte2Humen(vSize)}" }
          fileLines.foreach(y => fileCondStr = "<p>"+fileCondStr+"</p>"+ y)
          
          warnContent = s"目录（${nodeInfo.dir}）下存在文件大小低于阈值${nodeInfo.sizeThreshold}" + s"(${nodeInfo.warnMessage})"
          errorInfo = fileCondStr
          //日志记录
          errorLog(warnContent)
          fileLines.foreach { errorLog(_)}
          flag = false
        }
      }else{
        val tmp1 = s"""检测到目录（${nodeInfo.dir}）符合命名要求的文件（${nodeInfo.filename}）个数为${files.size},少于阈值${nodeInfo.numThreshold}"""
        if(nodeInfo.warnMessage != null && nodeInfo.warnMessage.trim() != ""){
          warnContent = nodeInfo.warnMessage
          errorInfo = tmp1
        }else{
          warnContent = tmp1
        }
        errorLog(warnContent)
        errorLog(errorInfo)
        flag = false
      }
    } catch{
      case e: Exception =>
        e.printStackTrace()
        errorLog(nodeInfo.warnMessage)
        errorLog(e.getMessage)
        warnContent = nodeInfo.warnMessage
        errorInfo = e.getMessage
        flag = false
    }
    if(!flag) sendErrorMail(warnContent, errorInfo)
    flag
  }
  
  private def sendErrorMail(warnContent: String, errorInfo: String) = {
    val content = s"""
        <style> 
        .table-n {text-align: center; border-collapse: collapse;border:1px solid black}
        h3 {margin-bottom: 5px}
        a {color:red;font-weight:bold}
        </style> 
        <h3>实例<data-monitor/>节点执行失败,内容如下</h3>
        
          <table class="table-n" border="1">
            <tr><td>实例ID</td><td>${this.id}</td></tr>
            <tr><td>节点名称</td><td>${nodeInfo.name}</td></tr>
            <tr><td>告警信息</td><td><a>${warnContent}</a></td></tr>
            <tr><td>出错信息</td><td><a>${errorInfo}</a></td></tr>
            <tr><td>总重试次数</td><td>${nodeInfo.retryTimes}</td></tr>
            <tr><td>当前重试次数</td><td>${this.hasRetryTimes}</td></tr>
            <tr><td>重试间隔</td><td>${nodeInfo.interval}秒</td></tr>
          </table>
          <a>&nbsp;<a>
        """
       actionActor.sendMailMsg(null, "【Akkaflow】file-monitor节点执行失败", content)
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

object FileMonitorNodeInstance {
  class DirNotExistException(msg: String) extends Exception(msg)
}