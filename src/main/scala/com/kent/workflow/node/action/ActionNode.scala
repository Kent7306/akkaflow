package com.kent.workflow.node.action

import com.kent.workflow.Workflow
import com.kent.workflow.node.Node
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods

abstract class ActionNode(name: String) extends Node(name)  {
  var retryTimes:Int = 0
  var interval:Int = 0
  var timeout:Int = -1
  var host:String = "-1"
  var ok: String = _
  var error: String = _ 
  var label: String = _
  //是否忽略
  var isIgnore: Boolean = false
  /**
   * 得到行动节点内部信息的json串
   */
  protected def toJsonString(): String

  def toJson(): String = {
    val actionInfoJson = ("host" -> host) ~
    ("retry-times" -> retryTimes) ~
    ("timeout" -> timeout) ~
    ("interval" -> interval) ~
    ("is-ignore" -> isIgnore) ~
    ("ok" -> ok) ~ ("error" -> error)
       
    val nodeInsideJson = JsonMethods.parse(toJsonString())
    val json = actionInfoJson.merge(nodeInsideJson)
    JsonMethods.pretty(JsonMethods.render(json))
  }
  def checkIntegrity(wf: Workflow): Unit = {
    if(wf.nodeList.filter{_.name == ok}.size == 0){
      throw new Exception(s"指向下一个成功节点${ok}不存在")
    }
    if(error!=null && error.trim()!="" && wf.nodeList.filter{_.name == error}.size == 0){
      throw new Exception(s"指向下一个失败节点${error}不存在")
    }
  }
}

object ActionNode {
  def apply(node: scala.xml.Node): ActionNode = {
    val nameOpt = node.attribute("name")
		val retryOpt = node.attribute("retry-times")
	  val intervalOpt = node.attribute("interval")
		val timeoutOpt = node.attribute("timeout")
		val hostOpt = node.attribute("host")
		val isIgnoreOpt = node.attribute("is-ignore")
		
		
		
    if((node \ "@name").size != 1){    
  	  throw new Exception("存在action未配置name属性")
    }else if((node \ "ok").size != 1){
      throw new Exception("[action] "+nameOpt.get.text+":未配置[ok]标签")
    }else if((node \ "ok" \ "@to").size != 1){
       throw new Exception("[action] "+nameOpt.get.text+":-->[ok]:未配置name属性")
    }
    var actionNode: ActionNode = null

    val childNode = (node \ "_")(0)
    childNode match {
      case <shell>{content @ _*}</shell> => 
        actionNode = ShellNode(nameOpt.get.text, childNode)
      case <script>{content @ _*}</script> => 
        actionNode = ScriptNode(nameOpt.get.text, childNode)
      case <file-monitor>{content @ _*}</file-monitor> => 
        actionNode = FileMonitorNode(nameOpt.get.text, childNode)
      case <file-executor>{content @ _*}</file-executor> => 
        actionNode = FileExecutorNode(nameOpt.get.text, childNode)
      case <data-monitor>{content @ _*}</data-monitor> => 
        actionNode = DataMonitorNode(nameOpt.get.text, childNode)
      case <sql>{content @ _*}</sql> => 
        actionNode = SqlNode(nameOpt.get.text, childNode) 
      case <transfer>{content @ _*}</transfer> => 
        actionNode = TransferNode(nameOpt.get.text, childNode) 
      case <email>{content @ _*}</email> => 
        actionNode = EmailNode(nameOpt.get.text, childNode) 
      case <metadata>{content @ _*}</metadata> => 
        actionNode = MetadataNode(nameOpt.get.text, childNode) 
      case _ => 
        throw new Exception(s"该[action:${nameOpt.get}]的类型不存在")
    }
    
    actionNode.retryTimes = if(!retryOpt.isEmpty) retryOpt.get.text.toInt else actionNode.retryTimes 
    actionNode.isIgnore = if(!isIgnoreOpt.isEmpty) isIgnoreOpt.get.text.toBoolean else actionNode.isIgnore
    actionNode.interval = if(!intervalOpt.isEmpty) intervalOpt.get.text.toInt else actionNode.interval
    actionNode.timeout = if(!timeoutOpt.isEmpty) timeoutOpt.get.text.toInt else actionNode.timeout
    actionNode.host = if(!hostOpt.isEmpty) hostOpt.get.text else actionNode.host
    actionNode.ok = (node \ "ok" \ "@to").text
    actionNode.label = childNode.label
    if((node \ "error").size == 1 && (node \ "error" \ "@to").size == 1){
    	actionNode.error = (node \ "error" \ "@to").text      
    }
    val descOpt = node.attribute("desc")
    if(descOpt != None) actionNode.desc = descOpt.get.text
    actionNode
    
  }
}