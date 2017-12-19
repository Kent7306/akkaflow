package com.kent.workflow.actionnode

import com.kent.workflow.node.ActionNode
import org.json4s.jackson.JsonMethods
import com.kent.util.Util

class FileWatcherNode(name: String) extends ActionNode(name) {
    var dir: String = _
    var numThreshold: Int = 1
    var filename: String = _
    var sizeThreshold: String = _
    var warnMessage: String = _
    var isWarnMsgEnable: Boolean = false
  
  override def getJson(): String = {
    import com.kent.util.Util._
    s"""{
       "file":{"dir":${transJsonStr(dir)},"num-threshold":${numThreshold},"name":${transJsonStr(filename)}},
       "size-warn-message":{"enable":${isWarnMsgEnable},"size-threshold":"${sizeThreshold}",
       "warn-msg":${transJsonStr(warnMessage)}}
     }"""
  }
  
}

object FileWatcherNode {
  def apply(name: String): FileWatcherNode = new FileWatcherNode(name)
  def apply(name:String, node: scala.xml.Node): FileWatcherNode = {
	  val fwan = FileWatcherNode(name)
	  val fileOpt = (node \ "file")
	  val sizeWarnMsgOpt = (node \ "size-warn-message")
	  if(!fileOpt.isEmpty) {
	    fwan.dir = fileOpt(0).attribute("dir").get.text
	    fwan.filename = fileOpt(0).text
	    fwan.numThreshold = if(fileOpt(0).attribute("num-threshold").isEmpty) fwan.numThreshold 
	                        else fileOpt(0).attribute("num-threshold").get.text.toInt
	  } else {
	    throw new Exception(s"节点[file-watcher: ${name}] 未配置<file>子标签")
	  }
	  if(!sizeWarnMsgOpt.isEmpty) {
	    fwan.sizeThreshold = sizeWarnMsgOpt(0).attribute("size-threshold").get.text
	    fwan.isWarnMsgEnable = if(sizeWarnMsgOpt(0).attribute("enable").get.text.toLowerCase == "true") true else false
	    fwan.warnMessage = sizeWarnMsgOpt(0).text
	  }
	  
	  fwan
  }
}