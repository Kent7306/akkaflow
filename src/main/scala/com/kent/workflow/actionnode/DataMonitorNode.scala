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
	        "jdbc-url":${transJsonStr(source.dbInfo._1)},
	        "username":${transJsonStr(source.dbInfo._2)},
	        "password":${transJsonStr(source.dbInfo._3)},
	        "content":${transJsonStr(source.content)}
	      }
	    }"""
	  if(minThre != null){
	    assembleStr += s"""
	      ,"min-thredshold":{
	        "type":${transJsonStr(minThre.stype.toString())},
	        "jdbc-url":${transJsonStr(minThre.dbInfo._1)},
	        "username":${transJsonStr(minThre.dbInfo._2)},
	        "password":${transJsonStr(minThre.dbInfo._3)},
	        "content":${transJsonStr(minThre.content)}
	      }
	    """
	  }
	  if(maxThre != null){
	    assembleStr += s"""
	      ,"max-thredshold":{
	        "type":${transJsonStr(maxThre.stype.toString())},
	        "jdbc-url":${transJsonStr(maxThre.dbInfo._1)},
	        "username":${transJsonStr(maxThre.dbInfo._2)},
	        "password":${transJsonStr(maxThre.dbInfo._3)},
	        "content":${transJsonStr(maxThre.content)}
	      },
	    """
	  }
	  assembleStr
  }
}

object DataMonitorNode {
  object SourceType extends Enumeration {
    type SourceType = Value
    val HIVE, ORACLE, MYSQL, COMMAND, NUM = Value
  }
  import com.kent.workflow.actionnode.DataMonitorNode.SourceType._
  case class MaxThreshold(stype: SourceType,content: String, dbInfo:Tuple3[String, String, String])
  case class MinThreshold(stype: SourceType,content: String, dbInfo:Tuple3[String, String, String])
  case class Source(stype: SourceType,content: String, dbInfo:Tuple3[String, String, String])
  
  
  
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
  	
	  if(scOpt.isEmpty){
	    throw new Exception(s"节点[data-monitor: ${name}] 未配置属性: category")
	  }
	  if(snOpt.isEmpty){
		  throw new Exception(s"节点[data-monitor: ${name}] 未配置属性: source-name")	    
	  }
	  node.category = scOpt.get.text
	  node.sourceName = snOpt.get.text
	  
	  //source
	  val sourceSeq = (xmlNode \ "source")
	  if(sourceSeq.size == 0){
	    throw new Exception(s"节点[data-monitor: ${name}] 未配置<source>子标签")	    
	  }
	  val sType = SourceType.withName(sourceSeq(0).attribute("type").get.text)
		val sUrl = if(sourceSeq(0).attribute("jdbc-url").isDefined) sourceSeq(0).attribute("jdbc-url").get.text else null
		val sUsername = if(sourceSeq(0).attribute("username").isDefined) sourceSeq(0).attribute("username").get.text else null
		val sPwd = if(sourceSeq(0).attribute("password").isDefined) sourceSeq(0).attribute("password").get.text else null
	  val sContent = sourceSeq(0).text
	  node.source = new Source(sType, sContent, (sUrl, sUsername, sPwd))
	  //max-threshold
	  val maxThrSeq = (xmlNode \ "max-threshold")
	  if(maxThrSeq.size > 0){
	    val tt = SourceType.withName(maxThrSeq(0).attribute("type").get.text)
	    val sUrl = if(sourceSeq(0).attribute("jdbc-url").isDefined) sourceSeq(0).attribute("jdbc-url").get.text else null
  		val sUsername = if(sourceSeq(0).attribute("username").isDefined) sourceSeq(0).attribute("username").get.text else null
  		val sPwd = if(sourceSeq(0).attribute("password").isDefined) sourceSeq(0).attribute("password").get.text else null
	    val ct = maxThrSeq(0).text
	    node.maxThre = new MaxThreshold(tt, ct, (sUrl, sUsername, sPwd))
	  }
	  //min-threshold
	  val minThrSeq = (xmlNode \ "min-threshold")
	  if(minThrSeq.size > 0){
	    val tt = SourceType.withName(minThrSeq(0).attribute("type").get.text)
	    val sUrl = if(sourceSeq(0).attribute("jdbc-url").isDefined) sourceSeq(0).attribute("jdbc-url").get.text else null
  		val sUsername = if(sourceSeq(0).attribute("username").isDefined) sourceSeq(0).attribute("username").get.text else null
  		val sPwd = if(sourceSeq(0).attribute("password").isDefined) sourceSeq(0).attribute("password").get.text else null
	    val ct = minThrSeq(0).text
	    node.minThre = new MinThreshold(tt, ct, (sUrl, sUsername, sPwd))
	  }
	  //warn-msg
	  val warnMsgSeq = (xmlNode \ "warn-msg")
	  if(warnMsgSeq.size > 0 && warnMsgSeq(0).text.trim() != ""){
	    node.warnMsg = warnMsgSeq(0).text
	  }
	  node 
  }
}