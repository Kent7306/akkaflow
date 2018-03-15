package com.kent.workflow.actionnode

import com.kent.workflow.node.ActionNode
import com.kent.workflow.node.NodeInstance
import com.kent.workflow.node.NodeInfo
import org.json4s.jackson.JsonMethods
import com.kent.util.Util
import com.kent.workflow.actionnode.DataMonitorNode.SourceType
import com.kent.workflow.actionnode.DataMonitorNode._
import java.sql.DriverManager
import java.sql.Connection
import java.sql.Statement
import com.kent.pub.Event.DBLink

class DataMonitorNode(name: String) extends ActionNode(name) {
  //数据源分类
  var category: String = _
  //数据源名称
  var sourceName: String = _
  //监控配置
  var source: Source = _
  //上限配置
  var maxThre: MaxThreshold = _
  //下限配置
  var minThre: MinThreshold = _
  //自定义告警消息
  var warnMsg: String = _
  //是否持久化
  var isSaved = false
  //是否超过限制就失败
  var isExceedError = true
  //时间标志
  var timeMark: String = _
  
  override def getJson(): String = {
	  import org.json4s.JsonDSL._
	  import com.kent.util.Util._
	  var assembleStr = s"""{
	      "category":${transJsonStr(category)},
	      "source-name":${transJsonStr(sourceName)},
	      "is-saved":${isSaved},
	      "is-exceed-error":${isExceedError},
	      "time-mark":${transJsonStr(timeMark)},
	      "warn-msg":${transJsonStr(warnMsg)},
	      "source":{
	        "type":${transJsonStr(source.stype.toString())},
	        "content":${transJsonStr(source.content)}
	      }
	    }"""
	  if(minThre != null){
	    assembleStr += s"""
	      ,"min-thredshold":{
	        "type":${transJsonStr(minThre.stype.toString())},
	        "content":${transJsonStr(minThre.content)}
	      }
	    """
	  }
	  if(maxThre != null){
	    assembleStr += s"""
	      ,"max-thredshold":{
	        "type":${transJsonStr(maxThre.stype.toString())},
	        "content":${transJsonStr(maxThre.content)}
	      },
	    """
	  }
	  assembleStr
  }
}

object DataMonitorNode {
  object DatabaseType extends Enumeration {
    type DatabaseType = Value
    val HIVE, ORACLE, MYSQL = Value
  }
  object SourceType extends Enumeration {
    type SourceType = Value
    val SQL, COMMAND, NUM = Value
  }
  
  import com.kent.workflow.actionnode.DataMonitorNode.SourceType._
  case class MinThreshold(stype: SourceType, content: String, dbLinkName: Option[String])
  case class Source(stype: SourceType, content: String, dbLinkName: Option[String])
  case class MaxThreshold(stype: SourceType, content: String, dbLinkName: Option[String])
  
  
  
  def apply(name: String): DataMonitorNode = new DataMonitorNode(name)
  def apply(name:String, xmlNode: scala.xml.Node): DataMonitorNode = {
	  val node = DataMonitorNode(name)
	  //属性
	  val scOpt = xmlNode.attribute("category")
	  val snOpt = xmlNode.attribute("source-name")
	  node.isSaved = if(xmlNode.attribute("is-saved").isDefined) 
  	      xmlNode.attribute("is-saved").get.text.toBoolean
  	  else
  	    node.isSaved
  	if(node.isSaved && xmlNode.attribute("time-mark").isEmpty){
  	  throw new Exception(s"节点[data-monitor: ${name}] 当is-saved=true的时候，必须配置time-mark属性")
  	}
	  node.timeMark = if(xmlNode.attribute("time-mark").isDefined)
  	    xmlNode.attribute("time-mark").get.text
  	  else 
  	    node.timeMark
  	val ieeOpt = xmlNode.attribute("is-exceed-error")
  	node.isExceedError = if(ieeOpt.isDefined) ieeOpt.get.text.toBoolean else node.isExceedError
  	
	  if(node.isSaved && scOpt.isEmpty){
	    throw new Exception(s"节点[data-monitor: ${name}] 未配置属性: category")
	  }
	  if(node.isSaved && snOpt.isEmpty){
		  throw new Exception(s"节点[data-monitor: ${name}] 未配置属性: source-name")	    
	  }
	  if(scOpt.isDefined) node.category = scOpt.get.text
	  if(snOpt.isDefined) node.sourceName = snOpt.get.text
	  
	  //source
	  val sourceSeq = (xmlNode \ "source")
	  if(sourceSeq.size == 0){
	    throw new Exception(s"节点[data-monitor: ${name}] 未配置<source>子标签")	    
	  }
	  val sType = SourceType.withName(sourceSeq(0).attribute("type").get.text)
		val sDBLinkNameOpt = if(sourceSeq(0).attribute("db-link").isDefined) Some(sourceSeq(0).attribute("db-link").get.text) else None
		if(sType == SQL && sDBLinkNameOpt.isEmpty){
		  throw new Exception(s"节点[data-monitor: ${name}] source的type为SQL时，须配置<db-link>子标签")	 
		}
	  val sContent = sourceSeq(0).text
	  node.source = new Source(sType, sContent, sDBLinkNameOpt)
	  //max-threshold
	  val maxThrSeq = (xmlNode \ "max-threshold")
	  if(maxThrSeq.size > 0){
	    val tt = SourceType.withName(maxThrSeq(0).attribute("type").get.text)
	    val maxDBLinkNameOpt = if(maxThrSeq(0).attribute("db-link").isDefined) Some(maxThrSeq(0).attribute("db-link").get.text) else None
  		if(tt == SQL && maxDBLinkNameOpt.isEmpty){
  		  throw new Exception(s"节点[data-monitor: ${name}] max-threshold的type为SQL时，须配置<db-link>子标签")	 
  		}
	    val ct = maxThrSeq(0).text
	    node.maxThre = new MaxThreshold(tt, ct, maxDBLinkNameOpt)
	  }
	  //min-threshold
	  val minThrSeq = (xmlNode \ "min-threshold")
	  if(minThrSeq.size > 0){
	    val tt = SourceType.withName(minThrSeq(0).attribute("type").get.text)
	    val minDBLinkNameOpt = if(minThrSeq(0).attribute("db-link").isDefined) Some(minThrSeq(0).attribute("db-link").get.text) else None
  		if(tt == SQL && minDBLinkNameOpt.isEmpty){
  		  throw new Exception(s"节点[data-monitor: ${name}] min-threshold的type为SQL时，须配置<db-link>子标签")	 
  		}
	    val ct = minThrSeq(0).text
	    node.minThre = new MinThreshold(tt, ct, minDBLinkNameOpt)
	  }
	  //warn-msg
	  val warnMsgSeq = (xmlNode \ "warn-msg")
	  if(warnMsgSeq.size > 0 && warnMsgSeq(0).text.trim() != ""){
	    node.warnMsg = warnMsgSeq(0).text
	  }
	  node 
  }
}