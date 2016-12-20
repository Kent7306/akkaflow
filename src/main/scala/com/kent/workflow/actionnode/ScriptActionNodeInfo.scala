package com.kent.workflow.actionnode

import com.kent.workflow.node.ActionNodeInfo
import com.kent.workflow.node.NodeInstance
import com.kent.workflow.node.NodeInfo
import org.json4s.jackson.JsonMethods

class ScriptActionNodeInfo(name: String) extends ActionNodeInfo(name) {
  var location: String = _
  var content: String = _
  
  def createInstance(workflowInstanceId: String): ScriptActionNodeInstance = {
    val sani = ScriptActionNodeInstance(this) 
    sani.id = workflowInstanceId
    sani
  }

  def deepClone(): ScriptActionNodeInfo = {
    val fn = ScriptActionNodeInfo(name)
    deepCloneAssist(fn)
    fn
  }
  def deepCloneAssist(san: ScriptActionNodeInfo): ScriptActionNodeInfo = {
    super.deepCloneAssist(san)
    san.location = location
    san.content = content
    san
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
    val c2 = JsonMethods.parse(s""" {"location":"${location}"}, {"content":"${content}"}""")
    println(c2)
    val c3 = c1.merge(c2)
    JsonMethods.pretty(JsonMethods.render(c3))
  }
}

object ScriptActionNodeInfo {
  def apply(name: String): ScriptActionNodeInfo = new ScriptActionNodeInfo(name)
  def apply(name:String, node: scala.xml.Node): ScriptActionNodeInfo = parseXmlNode(name, node)
  
  def parseXmlNode(name: String, node: scala.xml.Node): ScriptActionNodeInfo = {
    val location = (node \ "location")(0).text
    val content = (node \ "content")(0).text
	  val san = ScriptActionNodeInfo(name)
	  san.location = location
	  san.content = content
	  san
  }
}