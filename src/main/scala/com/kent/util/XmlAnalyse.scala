package com.kent.util

import scala.xml.XML
import com.kent.workflow.node.NodeInfo

object XmlAnalyse extends App{
	//implicit def NodeTagTypeToString(a: NodeTagType.Value):String = a.toString()
  def loadFile(filePath: String): Unit = {
    val xmlObj = XML.loadFile(filePath)
    val a = (xmlObj \ "action")
    println(a)
    
  }
  
  loadFile("F:\\git_resposit\\akkaflow\\xmlconfig\\workflow\\system\\wf_rm_tmp_file.xml")
  
}