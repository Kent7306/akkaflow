package com.kent.util

import scala.xml.XML
import com.kent.workflow.node.NodeInfo

object XmlAnalyse extends App{
	//implicit def NodeTagTypeToString(a: NodeTagType.Value):String = a.toString()
  def loadFile(filePath: String): Unit = {
    val xmlObj = XML.loadFile(filePath)
    val nodeList = (xmlObj \ "_").map(NodeInfo(_)).toList
    
    nodeList.foreach { x => println(x.name) }
  }
  
  loadFile("J:/wf.xml")
  
}