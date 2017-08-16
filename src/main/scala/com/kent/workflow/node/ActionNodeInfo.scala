package com.kent.workflow.node

import com.kent.workflow.actionnode._
import org.json4s.jackson.JsonMethods

abstract class ActionNodeInfo(name: String) extends NodeInfo(name)  {
  var retryTimes:Int = 0
  var interval:Int = 0
  var timeout:Int = -1
  var host:String = "-1"
  var ok: String = _
  var error: String = _ 
  
  override def setContent(contentStr: String){
    val content = JsonMethods.parse(contentStr)
    import org.json4s._
    implicit val formats = DefaultFormats
    this.host = (content \ "host").extract[String]
    this.retryTimes = (content \ "retry-times").extract[Int]
    this.interval = (content \ "interval").extract[Int]
    this.timeout = (content \ "timeout").extract[Int]
    this.ok = (content \ "ok").extract[String]
		this.error = (content \ "error").extract[String]
  }
  override def getContent(): String = {
    s"""
      {"host":"${host}",
       "retry-times":${retryTimes},
       "timeout":${timeout},
       "interval":${interval},
       "ok":"${ok}",
       "error":"${if(error == null) "" else error}"
      }
    """
  }

}

object ActionNodeInfo {  
  def apply(node: scala.xml.Node): ActionNodeInfo = parseXmlNode(node);
  
  def parseXmlNode(node: scala.xml.Node):ActionNodeInfo = {
    val nameOpt = node.attribute("name")
		val retryOpt = node.attribute("retry-times")
	  val intervalOpt = node.attribute("interval")
		val timeoutOpt = node.attribute("timeout")
		val hostOpt = node.attribute("host")
    if((node \ "@name").size != 1){    
  	  throw new Exception("存在action未配置name属性")
    }else if((node \ "ok").size != 1){
      throw new Exception("[action] "+nameOpt.get.text+":未配置[ok]标签")
    }else if((node \ "ok" \ "@to").size != 1){
       throw new Exception("[action] "+nameOpt.get.text+":-->[ok]:未配置name属性")
    }
    
    var actionNode: ActionNodeInfo = null

    val childNode = (node \ "_")(0)
    childNode match {
      case <shell>{content @ _*}</shell> => 
        actionNode = ShellNode(nameOpt.get.text, childNode)
      case <script>{content @ _*}</script> => 
        actionNode = ScriptNode(nameOpt.get.text, childNode)
      case <file-watcher>{content @ _*}</file-watcher> => 
        actionNode = FileWatcherNode(nameOpt.get.text, childNode)
      case <file-executor>{content @ _*}</file-executor> => 
        actionNode = FileExecutorNode(nameOpt.get.text, childNode)
      case <sub-workflow>{content @ _*}</sub-workflow> => 
        ???
      case _ => 
        throw new Exception(s"该[action:${nameOpt.get}]的类型不存在")
    }
    
    actionNode.retryTimes = if(!retryOpt.isEmpty) retryOpt.get.text.toInt else actionNode.retryTimes 
    actionNode.interval = if(!intervalOpt.isEmpty) intervalOpt.get.text.toInt else actionNode.interval
    actionNode.timeout = if(!timeoutOpt.isEmpty) timeoutOpt.get.text.toInt else actionNode.timeout
    actionNode.host = if(!hostOpt.isEmpty) hostOpt.get.text else actionNode.host
    actionNode.ok = (node \ "ok" \ "@to").text
    if((node \ "error").size == 1 && (node \ "error" \ "@to").size == 1){
    	actionNode.error = (node \ "error" \ "@to").text      
    }
    val descOpt = node.attribute("desc")
    if(descOpt != None) actionNode.desc = descOpt.get.text
    actionNode
    
  }
}