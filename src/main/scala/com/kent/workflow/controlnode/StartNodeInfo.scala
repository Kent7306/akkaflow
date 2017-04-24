package com.kent.workflow.controlnode

import com.kent.workflow.node.ControlNodeInfo
import com.kent.workflow.node.NodeInstance
import java.sql.Connection
import java.sql.ResultSet
import org.json4s.jackson.JsonMethods

class StartNodeInfo(name: String) extends ControlNodeInfo(name){
  var to: String = _

  override def createInstance(workflowInstanceId: String): StartNodeInstance = {
    val sni = StartNodeInstance(this.deepCloneAs[StartNodeInfo])
    sni.id = workflowInstanceId
    sni
  }
  override def setContent(contentStr: String){
    val content = JsonMethods.parse(contentStr)
    import org.json4s._
    implicit val formats = DefaultFormats
    val to = (content \ "to").extract[String]
    this.to = to
  }
  
  override def getContent(): String = {
    s"""{"to":"${to}"}"""
  }
}

object StartNodeInfo {
  def apply(name: String): StartNodeInfo = new StartNodeInfo(name)
  
  def apply(name: String, to: String): StartNodeInfo = {
    val sn = StartNodeInfo(name)
    sn.to = to
    sn
  }
  def apply(node: scala.xml.Node):StartNodeInfo = parseXmlNode(node)
  /**
   * 解析xml为一个对象
   */
  def parseXmlNode(node: scala.xml.Node): StartNodeInfo = {
      val nameOpt = node.attribute("name")
      val toOpt = node.attribute("to")
      
      if(nameOpt == None){
        throw new Exception("节点<start/>未配置name属性")
      }else if(toOpt == None){
        throw new Exception("节点<start/>未配置opt属性")
      }
      val sn = StartNodeInfo(nameOpt.get.text, toOpt.get.text)
      sn
  }
}