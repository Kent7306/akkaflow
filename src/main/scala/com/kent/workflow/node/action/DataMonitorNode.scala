package com.kent.workflow.node.action

import com.kent.workflow.node.action.DataMonitorNode._

class DataMonitorNode(name: String) extends ActionNode(name) {
  //监控配置
  var source: Source = _
  //上限配置
  var maxThre: MaxThreshold = _
  //下限配置
  var minThre: MinThreshold = _
  //自定义告警消息
  var warnMsg: String = _
  override def toJsonString(): String = {
	  val assembleStr = s"""{
	      "data":null
	    }"""
	  assembleStr
  }
}

object DataMonitorNode {
  object SourceType extends Enumeration {
    type SourceType = Value
    val SQL, COMMAND, NUM = Value
  }
  
  import com.kent.workflow.node.action.DataMonitorNode.SourceType._
  case class MinThreshold(stype: SourceType, content: String, dbLinkName: Option[String])
  case class Source(stype: SourceType, content: String, dbLinkName: Option[String])
  case class MaxThreshold(stype: SourceType, content: String, dbLinkName: Option[String])
  
  
  
  def apply(name: String): DataMonitorNode = new DataMonitorNode(name)
  def apply(name:String, xmlNode: scala.xml.Node): DataMonitorNode = {
	  val node = DataMonitorNode(name)

	  //source
	  val sourceSeq = xmlNode \ "source"
	  if(sourceSeq.isEmpty){
	    throw new Exception(s"节点[data-monitor: ${name}] 未配置<source>子标签")	    
	  }
	  val sType = SourceType.withName(sourceSeq(0).attribute("type").get.text)
		val sDBLinkNameOpt = if(sourceSeq(0).attribute("db-link").isDefined) Some(sourceSeq(0).attribute("db-link").get.text) else None
		if(sType == SQL && sDBLinkNameOpt.isEmpty){
		  throw new Exception(s"节点[data-monitor: ${name}] source的type为SQL时，须配置<db-link>子标签")	 
		}
	  val sContent = sourceSeq(0).text
	  node.source = Source(sType, sContent, sDBLinkNameOpt)
	  //max
	  val maxThrSeq = xmlNode \ "max"
	  if(maxThrSeq.nonEmpty){
	    val tt = SourceType.withName(maxThrSeq(0).attribute("type").get.text)
	    val maxDBLinkNameOpt = if(maxThrSeq(0).attribute("db-link").isDefined) Some(maxThrSeq(0).attribute("db-link").get.text) else None
  		if(tt == SQL && maxDBLinkNameOpt.isEmpty){
  		  throw new Exception(s"节点[data-monitor: ${name}] max的type为SQL时，须配置<db-link>子标签")
  		}
	    val ct = maxThrSeq(0).text
	    node.maxThre = MaxThreshold(tt, ct, maxDBLinkNameOpt)
	  }
	  //min
	  val minThrSeq = xmlNode \ "min"
	  if(minThrSeq.nonEmpty){
	    val tt = SourceType.withName(minThrSeq(0).attribute("type").get.text)
	    val minDBLinkNameOpt = if(minThrSeq(0).attribute("db-link").isDefined) Some(minThrSeq(0).attribute("db-link").get.text) else None
  		if(tt == SQL && minDBLinkNameOpt.isEmpty){
  		  throw new Exception(s"节点[data-monitor: ${name}] min的type为SQL时，须配置<db-link>子标签")
  		}
	    val ct = minThrSeq(0).text
	    node.minThre = new MinThreshold(tt, ct, minDBLinkNameOpt)
	  }
	  //warn-msg
	  val warnMsgSeq = xmlNode \ "warn-msg"
	  if(warnMsgSeq.size > 0 && warnMsgSeq(0).text.trim() != ""){
	    node.warnMsg = warnMsgSeq(0).text
	  }
	  node 
  }
}