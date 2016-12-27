package com.kent.workflow.actionnode

import com.kent.workflow.node.ActionNodeInfo
import org.json4s.jackson.JsonMethods
import com.kent.util.Util

class FileWatcherActionNodeInfo(name: String) extends ActionNodeInfo(name) {
    var dir: String = _
    var numThreshold: Int = 1
    var filename: String = _
    var sizeThreshold: String = _
    var warnMessage: String = _
  
    def createInstance(workflowInstanceId: String): FileWatcherActionNodeInstance = {
       val fwani = FileWatcherActionNodeInstance(this)
        fwani.id = workflowInstanceId
        fwani
    }

  def deepClone(): FileWatcherActionNodeInfo = {
    val fwan = FileWatcherActionNodeInfo(name)
    deepCloneAssist(fwan)
    fwan
  }
  def deepCloneAssist(fwan: FileWatcherActionNodeInfo): FileWatcherActionNodeInfo = {
    super.deepCloneAssist(fwan)
    fwan.dir = dir
    fwan.numThreshold = numThreshold
    fwan.filename = filename
    fwan.sizeThreshold = sizeThreshold
    fwan.warnMessage = warnMessage
    fwan
  }
  override def setContent(contentStr: String){
	  super.setContent(contentStr)
    val content = JsonMethods.parse(contentStr)
    import org.json4s._
    implicit val formats = DefaultFormats
    this.dir = (content \ "file" \ "dir").extract[String]
	  this.numThreshold = (content \ "file" \ "num-threshold").extract[Int]
	  this.filename = (content \ "file" \ "name").extract[String]
	  this.sizeThreshold = (content \ "size-warn-message" \ "size-threshold").extract[String]
		this.warnMessage = (content \ "size-warn-message" \ "warn-msg").extract[String]
	  
  }
  
  override def getContent(): String = {
    val contentStr = super.getContent()
    val c1 = JsonMethods.parse(contentStr)
    val c2 = JsonMethods.parse(s"""{
         "file":{"dir":"${dir}","num-threshold":${numThreshold},"name":"${filename}"},
         "size-warn-message":{"size-threshold":"${sizeThreshold}","warn-msg":"${Util.transformJsonStr(warnMessage)}"}
       }""")
    val c3 = c1.merge(c2)
    JsonMethods.pretty(JsonMethods.render(c3))
  }
  
}

object FileWatcherActionNodeInfo {
  def apply(name: String): FileWatcherActionNodeInfo = new FileWatcherActionNodeInfo(name)
  def apply(name:String, node: scala.xml.Node): FileWatcherActionNodeInfo = parseXmlNode(name, node)
  
  def parseXmlNode(name: String, node: scala.xml.Node): FileWatcherActionNodeInfo = {
	  val fwan = FileWatcherActionNodeInfo(name)
	  val fileOpt = (node \ "file")
	  val sizeWarnMsgOpt = (node \ "size-warn-message")
	  if(!fileOpt.isEmpty) {
	    fwan.dir = fileOpt(0).attribute("dir").get.text
	    fwan.numThreshold = if(fileOpt(0).attribute("num-threshold").isEmpty) fwan.numThreshold 
	                        else fileOpt(0).attribute("num-threshold").get.text.toInt
	  } else {
	    throw new Exception(s"节点[FileWatcherActionNodeInfo: ${name}] 未配置<file>子标签")
	  }
	  if(!sizeWarnMsgOpt.isEmpty) {
	    fwan.sizeThreshold = sizeWarnMsgOpt(0).attribute("size-threshold").get.text
	    fwan.warnMessage = sizeWarnMsgOpt(0).text
	  }
	  
	  fwan
  }
}