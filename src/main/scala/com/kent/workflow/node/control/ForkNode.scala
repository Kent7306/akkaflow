package com.kent.workflow.node.control

import com.kent.workflow.node.NodeInstance
import java.sql.Connection

import com.kent.workflow.node.Node
import java.sql.ResultSet

import com.kent.util.Util
import org.json4s.jackson.JsonMethods
import org.json4s.JsonAST.JString
import com.kent.workflow.Workflow

class ForkNode(name: String) extends ControlNode(name){
  var pathList: List[String] = List()
  
  override def toJson(): String = {
    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._
    val pathStr = compact(render(pathList))
    s"""{"paths":${pathStr}}"""
  }
  
  def checkIntegrity(wf: Workflow): Unit = {
    pathList.foreach{ path =>
      if(wf.nodeList.filter{_.name == path}.size == 0){
        	throw new Exception(s"指向下一个节点${path}不存在")
      }      
    }
    
  }
  
}

object ForkNode {
  def apply(name: String): ForkNode = new ForkNode(name)
  def apply(node: scala.xml.Node): ForkNode = {
	  val nameOpt = node.attribute("name")
		val pathList = (node \ "path").map { x => x.attribute("to").get.text }.toList
    if((node \ "@name").size != 1){    
  	  throw new Exception("存在fork未配置name属性")
    }else if(pathList.size <= 0){
       throw new Exception("[fork] "+nameOpt.get.text+":-->[error]:未配置path子标签")
    }
    val fn = ForkNode(nameOpt.get.text)
    fn.pathList = pathList
    fn
  }
}