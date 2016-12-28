package com.kent.workflow.actionnode

import com.kent.workflow.node.ActionNodeInstance
import java.util.Date
import com.kent.coordinate.ParamHandler
import java.io.File
import com.kent.util.Util

class FileWatcherActionNodeInstance(override val nodeInfo: FileWatcherActionNodeInfo)  extends ActionNodeInstance(nodeInfo)  {
  def deepClone(): FileWatcherActionNodeInstance = {
    val hsani = FileWatcherActionNodeInstance(nodeInfo)
    deepCloneAssist(hsani)
    hsani
  }

  def execute(): Boolean = {
    val pattern = nodeInfo.filename.r
    val filesize = Util.convertHumen2Byte(nodeInfo.sizeThreshold)
    
    if(nodeInfo.dir.toLowerCase().matches("hdfs:")){
      ???
    }else if(nodeInfo.dir.toLowerCase().matches("sftp:")){
      ???
    }else if(nodeInfo.dir.toLowerCase().matches("sftp:")){
      ???
    }else {
      val file = new File(nodeInfo.dir)
      if(file.isDirectory() && file.exists()) {
        val files = file.listFiles().filter { x => !pattern.findFirstIn(x.getName).isEmpty}.toList
        if(files.size >= nodeInfo.numThreshold){
          val smallerFiles = files.filter { _.length() < filesize }.toList
          if(smallerFiles.size > 0){
            ???  //邮件告警
          }
          true
        } else false
      } else false
    }
  }

  def kill(): Boolean = {
    ???
  }

  def replaceParam(param: Map[String, String]): Boolean = {
    nodeInfo.dir = ParamHandler(new Date()).getValue(nodeInfo.dir, param)
    nodeInfo.filename = ParamHandler(new Date()).getValue(nodeInfo.filename, param)
    nodeInfo.warnMessage = ParamHandler(new Date()).getValue(nodeInfo.warnMessage, param)
    true
  }
}

object FileWatcherActionNodeInstance {
  def apply(fwan: FileWatcherActionNodeInfo): FileWatcherActionNodeInstance = {
    val cfwan = fwan.deepClone()
    val fwani = new FileWatcherActionNodeInstance(cfwan)
    fwani
  }
}