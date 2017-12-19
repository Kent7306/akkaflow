package com.kent.workflow.controlnode

import com.kent.workflow.node.ControlNode
import com.kent.workflow.node.NodeInstance
import java.sql.Connection
import com.kent.workflow.node.NodeInfo
import java.sql.ResultSet
import com.kent.util.Util
import org.json4s.jackson.JsonMethods
import org.json4s.JsonAST.JString

class ForkNode(name: String) extends ControlNode(name){
  var pathList: List[String] = List()
  
  override def getJson(): String = {
    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._
    val pathStr = compact(render(pathList))
    s"""{"paths":${pathStr}}"""
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