package com.kent.workflow.actionnode

import com.kent.workflow.node.ActionNodeInfo
import com.kent.workflow.node.NodeInstance
import com.kent.workflow.node.NodeInfo
import org.json4s.jackson.JsonMethods
import com.kent.util.Util

class ScriptNode(name: String) extends ActionNodeInfo(name) {
  var location: String = _
  var content: String = _
  
  def createInstance(workflowInstanceId: String): ScriptNodeInstance = {
    val sani = ScriptNodeInstance(this.deepCloneAs[ScriptNode]) 
    sani.id = workflowInstanceId
    sani
  }
  
  override def setContent(contentStr: String){
	  super.setContent(contentStr)
    val content = JsonMethods.parse(contentStr)
    import org.json4s._
    implicit val formats = DefaultFormats
    this.location = (content \ "location").extract[String]
    this.content = (content \ "content").extract[String]
  }
  
  override def getContent(): String = {
    val c1 = JsonMethods.parse(super.getContent())
    val c2 = JsonMethods.parse(s""" {"location":"${location}","content":"${Util.transformJsonStr(content)}"}""")
    val c3 = c1.merge(c2)
    JsonMethods.pretty(JsonMethods.render(c3))
  }
}

object ScriptNode {
  def apply(name: String): ScriptNode = new ScriptNode(name)
  def apply(name:String, node: scala.xml.Node): ScriptNode = parseXmlNode(name, node)
  
  def parseXmlNode(name: String, node: scala.xml.Node): ScriptNode = {
	  val san = ScriptNode(name)
	  val locaOpt = (node \ "location")
	  san.location = if(locaOpt.isEmpty) "" else locaOpt(0).text
	  san.content = (node \ "content")(0).text
	  san
  }
}