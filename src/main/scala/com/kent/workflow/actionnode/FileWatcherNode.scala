package com.kent.workflow.actionnode

import com.kent.workflow.node.ActionNodeInfo
import org.json4s.jackson.JsonMethods
import com.kent.util.Util

class FileWatcherNode(name: String) extends ActionNodeInfo(name) {
    var dir: String = _
    var numThreshold: Int = 1
    var filename: String = _
    var sizeThreshold: String = _
    var warnMessage: String = _
    var isWarnMsgEnable: Boolean = false
  
    def createInstance(workflowInstanceId: String): FileWatcherNodeInstance = {
       val fwani = FileWatcherNodeInstance(this.deepCloneAs[FileWatcherNode])
        fwani.id = workflowInstanceId
        fwani
    }

  override def parseJsonStr(contentStr: String){
	  super.parseJsonStr(contentStr)
    val content = JsonMethods.parse(contentStr)
    import org.json4s._
    implicit val formats = DefaultFormats
    this.dir = (content \ "file" \ "dir").extract[String]
	  this.numThreshold = (content \ "file" \ "num-threshold").extract[Int]
	  this.filename = (content \ "file" \ "name").extract[String]
	  this.sizeThreshold = (content \ "size-warn-message" \ "size-threshold").extract[String]
		this.warnMessage = (content \ "size-warn-message" \ "warn-msg").extract[String]
	  this.isWarnMsgEnable =  (content \ "size-warn-message" \ "enable").extract[Boolean]
	  
  }
  
  override def assembleJsonStr(): String = {
    import com.kent.util.Util._
    val contentStr = super.assembleJsonStr()
    val c1 = JsonMethods.parse(contentStr)
    val c2 = JsonMethods.parse(s"""{
         "file":{"dir":${transJsonStr(dir)},"num-threshold":${numThreshold},"name":${transJsonStr(filename)}},
         "size-warn-message":{"enable":${isWarnMsgEnable},"size-threshold":"${sizeThreshold}",
         "warn-msg":${transJsonStr(warnMessage)}}
       }""")
    val c3 = c1.merge(c2)
    JsonMethods.pretty(JsonMethods.render(c3))
  }
  
}

object FileWatcherNode {
  def apply(name: String): FileWatcherNode = new FileWatcherNode(name)
  def apply(name:String, node: scala.xml.Node): FileWatcherNode = parseXmlNode(name, node)
  
  def parseXmlNode(name: String, node: scala.xml.Node): FileWatcherNode = {
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