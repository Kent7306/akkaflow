package com.kent.workflow.actionnode

import com.kent.workflow.node.ActionNodeInstance
import java.util.Date
import com.kent.coordinate.ParamHandler
import java.io.File

class FileWatcherActionNodeInstance(override val nodeInfo: FileWatcherActionNodeInfo)  extends ActionNodeInstance(nodeInfo)  {
  def deepClone(): FileWatcherActionNodeInstance = {
    val hsani = FileWatcherActionNodeInstance(nodeInfo)
    deepCloneAssist(hsani)
    hsani
  }

  def execute(): Boolean = {
    val pattern = this.nodeInfo.filename.r
    //val filesize = this.nodeInfo.sizeThreshold.matches(arg0)
    
    if(this.nodeInfo.dir.toLowerCase().matches("hdfs:")){
      ???
    }else if(this.nodeInfo.dir.toLowerCase().matches("sftp:")){
      ???
    }else if(this.nodeInfo.dir.toLowerCase().matches("sftp:")){
      ???
    }else {
      val file = new File(this.nodeInfo.dir)
      if(file.isDirectory() && file.exists()) {
        val files = file.listFiles().filter { x => !pattern.findFirstIn(x.getName).isEmpty}.toList
        if(files.size >= this.nodeInfo.numThreshold){
          //files.foreach { x => x. }
          true
        }else{
          false
        }
      } else {
        false
      }
      
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