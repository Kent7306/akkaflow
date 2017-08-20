package com.kent.workflow.controlnode

import com.kent.workflow.node.ControlNodeInfo
import com.kent.workflow.node.NodeInstance
import java.sql.Connection
import com.kent.workflow.node.NodeInfo
import java.sql.ResultSet
import com.kent.util.Util
import org.json4s.jackson.JsonMethods

class JoinNodeInfo(name: String) extends ControlNodeInfo(name)  {
  var to: String = _

  override def createInstance(workflowInstanceId: String): JoinNodeInstance = {
    val jni = JoinNodeInstance(this.deepCloneAs[JoinNodeInfo])
    jni.id = workflowInstanceId
    jni
  }

  override def parseJsonStr(contentStr: String){
    val content = JsonMethods.parse(contentStr)
    import org.json4s._
    implicit val formats = DefaultFormats
    val to = (content \ "to").extract[String]
    this.to = to
  }
  
  override def assembleJsonStr(): String = {
    s"""{"to":"${to}"}"""
  }
}

object JoinNodeInfo {
  def apply(name: String): JoinNodeInfo = new JoinNodeInfo(name)
  def apply(node: scala.xml.Node): JoinNodeInfo = parseXmlNode(node)
  
  def parseXmlNode(node: scala.xml.Node): JoinNodeInfo = {
	  val nameOpt = node.attribute("name")
	  val toOpt = node.attribute("to")
    if((node \ "@name").size != 1){    
  	  throw new Exception("存在join未配置name属性")
    }
    val jn = JoinNodeInfo(nameOpt.get.text) 
    jn.to = toOpt.get.text
    jn
  }
}