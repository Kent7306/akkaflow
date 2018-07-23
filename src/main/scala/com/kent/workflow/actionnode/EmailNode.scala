package com.kent.workflow.actionnode

import com.kent.workflow.node.ActionNode
import com.kent.util.Util._

class EmailNode(name: String) extends ActionNode(name) {
  var htmlContent: String = _
  def getJson(): String = {
    s""" {"html":${transJsonStr(htmlContent)}}"""
  }
}

object EmailNode {
  def apply(name: String): EmailNode = new EmailNode(name)
  def apply(name:String, xmlNode: scala.xml.Node): EmailNode = {
	  val node = EmailNode(name)
	  //html代码
	  node.htmlContent = 
	    if((xmlNode \ "html").size > 0){
	    (xmlNode \ "_").toString()
	  }else{
	    "<html>"+(xmlNode \ "_").toString()+"</html>"
	  }
	  node
  }
}