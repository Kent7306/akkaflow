package com.kent.workflow.actionnode

import com.kent.workflow.node.ActionNodeInfo
import com.kent.workflow.node.NodeInstance
import com.kent.workflow.node.NodeInfo
import org.json4s.jackson.JsonMethods
import com.kent.util.Util
import com.kent.workflow.actionnode.DataMonitorNode.SourceType
import com.kent.workflow.actionnode.DataMonitorNode._

class DataMonitorNode(name: String) extends ActionNodeInfo(name) {
  //数据源分类
  var sourceCategory: String = _
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
  //时间标志
  var timeMark: String = _
  
  //???
  def createInstance(workflowInstanceId: String): ScriptNodeInstance = {
    val sani = ScriptNodeInstance(this.deepCloneAs[ScriptNode]) 
    sani.id = workflowInstanceId
    sani
  }
  
  override def parseJsonStr(contentStr: String){
	  super.parseJsonStr(contentStr)
    val content = JsonMethods.parse(contentStr)
    import org.json4s._
    implicit val formats = DefaultFormats
    this.sourceCategory = (content \ "source-category").extract[String]
    this.sourceName = (content \ "source-category").extract[String]
	  this.isSaved = (content \ "is-saved").extract[Boolean]
	  this.timeMark = (content \ "time-mark").extract[String]
	  this.warnMsg = (content \ "warn-msg").extract[String]
    
	  val sourType = (content \ "source" \ "type").extract[String]
	  val sourContent = (content \ "source" \ "content").extract[String]
	  this.source = Source(SourceType.withName(sourType), sourContent)
	  
	  val maxtType = (content \ "max-thredshold" \ "type").extract[String]
	  val maxTContent = (content \ "max-thredshold" \ "content").extract[String]
	  val maxIsOverError = (content \ "max-thredshold" \ "is-over-error").extract[Boolean]
	  this.maxThre = MaxThreshold(SourceType.withName(maxtType),maxIsOverError, maxTContent)
	  
	  val mintType = (content \ "min-thredshold" \ "type").extract[String]
	  val minTContent = (content \ "min-thredshold" \ "content").extract[String]
	  val minIsOverError = (content \ "min-thredshold" \ "is-over-error").extract[Boolean]
	  this.minThre = MinThreshold(SourceType.withName(mintType),maxIsOverError, minTContent)
	  
    
  }
  
  override def assembleJsonStr(): String = {
    import org.json4s.jackson.JsonMethods._
	  import org.json4s.JsonDSL._
	  val c1 = JsonMethods.parse(super.assembleJsonStr())
	  val c2 = JsonMethods.parse(s"""{
	      "source-category":"${sourceCategory}",
	      "source-name":"${sourceName}",
	      "is-saved":${isSaved},
	      "time-mark":"${Util.transformJsonStr(timeMark)}",
	      "source":{
	        "type":"${source.stype}",
	        "content":"${Util.transformJsonStr(source.content)}"
	      },
	      "min-thredshold":{
	        "type":"${minThre.stype}",
	        "is-over-error":${minThre.isOverError},
	        "content":"${Util.transformJsonStr(minThre.content)}"
	      },
	      "max-thredshold":{
	        "type":"${maxThre.stype}",
	        "is-over-error":${maxThre.isOverError},
	        "content":"${Util.transformJsonStr(maxThre.content)}"
	      },
	      "warn-msg":"${Util.transformJsonStr(warnMsg)}"
	    }""")
    val c3 = c1.merge(c2)
    JsonMethods.pretty(JsonMethods.render(c3))
  }
}

object DataMonitorNode {
  object SourceType extends Enumeration {
    type SourceType = Value
    val HIVE, ORACLE, MYSQL, COMMAND, NUM = Value
  }
  import com.kent.workflow.actionnode.DataMonitorNode.SourceType._
  case class MaxThreshold(stype: SourceType, isOverError: Boolean, content: String)
  case class MinThreshold(stype: SourceType, isOverError: Boolean, content: String) 
  case class Source(stype: SourceType, content: String)
  
  def apply(name: String): DataMonitorNode = new DataMonitorNode(name)
  def apply(name:String, node: scala.xml.Node): DataMonitorNode = parseXmlNode(name, node)
  
  def parseXmlNode(name: String, xmlNode: scala.xml.Node): DataMonitorNode = {
	  val node = DataMonitorNode(name)
	  //属性
	  val scOpt = xmlNode.attribute("source-category")
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
	  if(scOpt.isEmpty){
	    throw new Exception(s"节点[data-monitor: ${name}] 未配置属性: source-category")
	  }else if(snOpt.isEmpty){
		  throw new Exception(s"节点[data-monitor: ${name}] 未配置属性: source-name")	    
	  }
	  
	  
	  //source
	  val sourceSeq = (xmlNode \ "souce")
	  if(sourceSeq.size == 0){
	    throw new Exception(s"节点[data-monitor: ${name}] 未配置<source>子标签")	    
	  }
	  val sType = SourceType.withName(sourceSeq(0).attribute("type").get.text)
	  val sContent = sourceSeq(0).text
	  node.source = new Source(sType, sContent)
	  //max-threshold
	  val maxThrSeq = (xmlNode \ "max-threshold")
	  if(maxThrSeq.size > 0){
	    val tt = SourceType.withName(maxThrSeq(0).attribute("type").get.text)
	    val et = maxThrSeq(0).attribute("is-over-error").get.text.toBoolean
	    val ct = maxThrSeq(0).text
	    node.maxThre = new MaxThreshold(tt, et, ct)
	  }
	  //min-threshold
	  val minThrSeq = (xmlNode \ "min-threshold")
	  if(minThrSeq.size > 0){
	    val tt = SourceType.withName(minThrSeq(0).attribute("type").get.text)
	    val et = minThrSeq(0).attribute("is-over-error").get.text.toBoolean
	    val ct = minThrSeq(0).text
	    node.minThre = new MinThreshold(tt, et, ct)
	  }
	  //warn-msg
	  val warnMsgSeq = (xmlNode \ "warn-msg")
	  if(warnMsgSeq.size > 0 && warnMsgSeq(0).text.trim() != ""){
	    node.warnMsg = warnMsgSeq(0).text
	  }
	  node 
  }
}