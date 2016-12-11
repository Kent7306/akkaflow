package com.kent.workflow.node

import com.kent.workflow.actionnode.HostScriptActionNodeInfo
import com.kent.workflow.actionnode.HostScriptActionNodeInfo

abstract class ActionNodeInfo(name: String) extends NodeInfo(name)  {
  var retryTimes:Int = 0
  var interval:Int = 0
  var timeout:Int = -1
  var host:String = "-1"
  var ok: String = _
  var error: String = _ 
  
  def deepCloneAssist(an: ActionNodeInfo): ActionNodeInfo = {
	  super.deepCloneAssist(an)
    an.retryTimes = retryTimes
    an.interval = interval
    an.timeout = timeout
    an.ok = ok
    an.error = error
    an
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
    }/*else if((node \ "error").size != 1){
      throw new Exception("[action] "+nameOpt.get.text+":未配置[error]标签")
    }else if((node \ "error" \ "@to").size != 1){
       throw new Exception("[action] "+nameOpt.get.text+":-->[error]:未配置name属性")
    }*/
    
    var actionNode: ActionNodeInfo = null
    //if((node \ "_").size != 1) throw new Exception(s"该[action:${nameOpt.get}]的子节点不唯一")

    val childNode = (node \ "_")(0)
    childNode match {
      case <host-script>{content @ _*}</host-script> => 
        actionNode = HostScriptActionNodeInfo(nameOpt.get.text, childNode)
      case <sub-workflow>{content @ _*}</sub-workflow> => 
        ???
      case _ => 
        throw new Exception(s"该[action:${nameOpt.get}]的类型不存在")
    }
    
    actionNode.retryTimes = if(!retryOpt.isEmpty) retryOpt.get.text.toInt else actionNode.retryTimes 
    actionNode.interval = if(!intervalOpt.isEmpty) intervalOpt.get.text.toInt else actionNode.interval
    actionNode.timeout = if(!retryOpt.isEmpty) timeoutOpt.get.text.toInt else actionNode.timeout
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