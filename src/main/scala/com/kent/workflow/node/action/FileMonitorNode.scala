package com.kent.workflow.node.action

import org.json4s.jackson.JsonMethods
import com.kent.util.Util
import com.kent.util.FileUtil

/**
 * <file-monitor>
 *     <file num-threshold="1" size-thresold="2MB">/home/you/app/dir/aa.sh</file>
 *     <warn-msg>请填写异常告警信息</warn-msg>
 * </file-monitor>
 */
class FileMonitorNode(name: String) extends ActionNode(name) {
    var dir: String = _
    var numThreshold: Int = 1
    var filename: String = _
    var sizeThreshold: String = "0B"
    var warnMessage: String = _
  
  override def toJsonString(): String = {
    import com.kent.util.Util._
    s"""{
       "file":{"dir":${transJsonStr(dir)},"min-num":${numThreshold},"name":${transJsonStr(filename)}},
       "size-warn-message":{"min-each-size":"${sizeThreshold}",
       "warn-msg":${transJsonStr(warnMessage)}}
     }"""
  }
  
}

object FileMonitorNode {
  def apply(name: String): FileMonitorNode = new FileMonitorNode(name)
  def apply(name:String, node: scala.xml.Node): FileMonitorNode = {
	  val fwan = FileMonitorNode(name)
	  val fileOpt = node \ "file"
	  val warnMsgOpt = node \ "warn-msg"
	  if(fileOpt.nonEmpty) {
  	  val (dir, baseName) = FileUtil.getDirAndBaseName(fileOpt(0).text)
  	  fwan.dir = dir
  	  fwan.filename = baseName
      fwan.numThreshold = if(fileOpt(0).attribute("min-num").isEmpty) fwan.numThreshold
	                        else fileOpt(0).attribute("min-num").get.text.toInt
	    fwan.sizeThreshold = if(fileOpt(0).attribute("min-each-size").isEmpty) fwan.sizeThreshold
	                        else fileOpt(0).attribute("min-each-size").get.text
	  } else {
	    throw new Exception(s"节点[file-monitor: ${name}] 未配置<file>子标签")
	  }
	  if(warnMsgOpt.nonEmpty) fwan.warnMessage = warnMsgOpt.head.text
	  fwan
  }
}