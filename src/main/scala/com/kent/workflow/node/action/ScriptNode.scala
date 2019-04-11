package com.kent.workflow.node.action

import com.kent.workflow.node.NodeInstance
import com.kent.workflow.node.Node
import org.json4s.jackson.JsonMethods
import com.kent.util.Util

class ScriptNode(name: String) extends ActionNode(name) {
  var paramLine: String = _
  var code: String = _
  var attachFiles = List[String]()
  
  override def toJsonString(): String = {
	  import org.json4s.JsonDSL._
	  import com.kent.util.Util._
	  val attStr = JsonMethods.compact(JsonMethods.render(attachFiles))
    s""" {"code":${transJsonStr(code)},
         "attach-list":${attStr},
         "param-line":${transJsonStr(paramLine)}
    }"""
  }
}

object ScriptNode {
  def apply(name: String): ScriptNode = new ScriptNode(name)
  def apply(name:String, xmlNode: scala.xml.Node): ScriptNode = {
	  val node = ScriptNode(name)
	  //代码
	  node.code = (xmlNode \ "code")(0).text
	  //附加文件，可选
	  var attchFilesSeq = (xmlNode \ "attach-list")
	  node.attachFiles = (attchFilesSeq \ "file").map(f => f(0).text).toList
	  //参数行
	  val paramLineSeq = (xmlNode \ "param-line")
	  if(paramLineSeq.size > 0){
		  node.paramLine = paramLineSeq(0).text
	  }
	  node 
  }
}