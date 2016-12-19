package com.kent.workflow.controlnode

import com.kent.workflow.node.ControlNodeInfo
import com.kent.workflow.node.NodeInstance
import java.sql.Connection
import com.kent.workflow.node.NodeInfo
import java.sql.ResultSet
import com.kent.util.Util
import org.json4s.jackson.JsonMethods
import org.json4s.JsonAST.JString

class ForkNodeInfo(name: String) extends ControlNodeInfo(name){
  var pathList: List[String] = List()

  def deepClone(): ForkNodeInfo = {
    val fn = ForkNodeInfo(name)
    fn.pathList = pathList.map (_.toString()).toList
    this.deepCloneAssist(fn)
    fn
  }

  override def createInstance(workflowInstanceId: String): ForkNodeInstance = {
    val fni = ForkNodeInstance(this)
    fni.id = workflowInstanceId
    fni
  }

  override def setContent(contentStr: String){
    val content = JsonMethods.parse(contentStr)
    this.pathList = (content \ "paths" \\ classOf[JString]).asInstanceOf[List[String]]
  }
  
  override def getContent(): String = {
    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._
    val pathStr = compact(render(pathList))
    s"""{"paths":${pathStr}}"""
  }
}

object ForkNodeInfo {
  def apply(name: String): ForkNodeInfo = new ForkNodeInfo(name)
  def apply(node: scala.xml.Node): ForkNodeInfo = parseXmlNode(node)
  
  def parseXmlNode(node: scala.xml.Node): ForkNodeInfo = {
	  val nameOpt = node.attribute("name")
		val pathList = (node \ "path").map { x => x.attribute("to").get.text }.toList
    if((node \ "@name").size != 1){    
  	  throw new Exception("存在fork未配置name属性")
    }else if(pathList.size <= 0){
       throw new Exception("[fork] "+nameOpt.get.text+":-->[error]:未配置path子标签")
    }
    val fn = ForkNodeInfo(nameOpt.get.text)
    fn.pathList = pathList
    fn
  }
}