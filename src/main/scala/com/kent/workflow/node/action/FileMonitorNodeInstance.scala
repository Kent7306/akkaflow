package com.kent.workflow.node.action

import java.io.File
import java.net.URI

import com.kent.util.{FileUtil, Util}
import com.kent.workflow.node.action.FileMonitorNodeInstance.DirNotExistException
import scala.concurrent.duration._

import scala.concurrent.Await

/**
  * 文件检测
  * @param nodeInfo
  */
class FileMonitorNodeInstance(override val nodeInfo: FileMonitorNode)  extends ActionNodeInstance(nodeInfo)  {
  val DATA_CHECK_INTERVAL = 10000


  def execute(): Boolean = {
    val flOptF = this.actionActor.getFileLink(nodeInfo.fileLink)
    val flOpt = Await.result(flOptF, 20 second)

    if (flOpt.isEmpty){
      throw new Exception(s"未找到名称为${nodeInfo.fileLink}的文件系统配置")
    }
    val operator = flOpt.get.getOperator()

    //双层检查
    val files = operator.getFuzzyFiles(nodeInfo.filename, nodeInfo.dir)
    Thread.sleep(DATA_CHECK_INTERVAL)
    val delayFiles = operator.getFuzzyFiles(nodeInfo.filename, nodeInfo.dir)

    if (files.size != delayFiles.size){
      throw new Exception(s"两次检测文件量不一致")
    }
    files.foreach{ case (name, size) =>
      if(!delayFiles.contains(name)) throw new Exception(s"两次检测文件名称不一致: ${name}")
      if(delayFiles(name) != size) throw new Exception(s"两次检测文件${name}大小不一致: ${delayFiles(name)}, ${size}")
    }

    //输出扫描文件信息日志
    files.foreach{ case(name, size) =>
      infoLog(s"扫描目录${nodeInfo.dir},文件信息如下")
      infoLog(s"文件 -> ${name} -> ${Util.convertByte2Humen(size)}")
    }

    //检测的文件个数要符合规定个数
    if(files.size >= nodeInfo.numThreshold){
      val smallFiles = files.filter{case (vFn, vSize) => vSize < Util.convertHumen2Byte(nodeInfo.sizeThreshold)}
      //存在文件大小低于阈值 并且 设置启动告警
      if(smallFiles.size > 0){
        var fileCondStr = s"目录（${nodeInfo.dir}）下存在文件大小低于阈值${nodeInfo.sizeThreshold}\n"
        fileCondStr += "异常文件列表\n"
        val fileLinesStr = smallFiles.map { case (vFn, vSize) => s"文件 -> ${vFn} -> ${Util.convertByte2Humen(vSize)}" }.mkString("\n")
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