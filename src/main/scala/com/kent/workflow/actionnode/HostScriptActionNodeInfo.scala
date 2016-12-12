package com.kent.workflow.actionnode

import com.kent.workflow.node.ActionNodeInfo
import com.kent.workflow.node.NodeInstance
import java.sql.Connection
import com.kent.workflow.node.NodeInfo
import java.sql.ResultSet
import java.sql.ResultSet
import org.json4s.jackson.JsonMethods

class HostScriptActionNodeInfo(name: String) extends ActionNodeInfo(name) {
  var script: String = _

  def deepClone(): HostScriptActionNodeInfo = {
    val fn = HostScriptActionNodeInfo(name)
    deepCloneAssist(fn)
    fn
  }
  def deepCloneAssist(hn: HostScriptActionNodeInfo): HostScriptActionNodeInfo = {
    super.deepCloneAssist(hn)
    hn.host = host
    hn.script = script
    hn
  }

  def createInstance(workflowInstanceId: String): HostScriptActionNodeInstance = {
    val hsani = HostScriptActionNodeInstance(this)
    hsani.id = workflowInstanceId
    hsani
  }

  override def setContent(contentStr: String){
	  super.setContent(contentStr)
    val content = JsonMethods.parse(contentStr)
    import org.json4s._
    implicit val formats = DefaultFormats
    val script = (content \ "script").extract[String]
    this.script = script
  }
  
  override def getContent(): String = {
    val contentStr = super.getContent()
    val c1 = JsonMethods.parse(contentStr)
    val c2 = JsonMethods.parse(s""" {"script":"${script}"}""")
    val c3 = c1.merge(c2)
    JsonMethods.pretty(JsonMethods.render(c3))
  }
}

object HostScriptActionNodeInfo {
  def apply(name: String): HostScriptActionNodeInfo = new HostScriptActionNodeInfo(name)
  def apply(name:String, node: scala.xml.Node): HostScriptActionNodeInfo = parseXmlNode(name, node)
  
  def parseXmlNode(name: String, node: scala.xml.Node): HostScriptActionNodeInfo = {
    val host = (node \ "host")(0).text
    val script = (node \ "script")(0).text
    
	  val hsan = HostScriptActionNodeInfo(name)
	  hsan.host = host
	  hsan.script = script
	  hsan
  }
}