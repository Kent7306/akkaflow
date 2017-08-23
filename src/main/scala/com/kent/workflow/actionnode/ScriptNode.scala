package com.kent.workflow.actionnode

import com.kent.workflow.node.ActionNodeInfo
import com.kent.workflow.node.NodeInstance
import com.kent.workflow.node.NodeInfo
import org.json4s.jackson.JsonMethods
import com.kent.util.Util

class ScriptNode(name: String) extends ActionNodeInfo(name) {
  var paramLine: String = _
  var code: String = _
  var attachFiles = List[String]()
  
  def createInstance(workflowInstanceId: String): ScriptNodeInstance = {
    val sani = ScriptNodeInstance(this.deepCloneAs[ScriptNode]) 
    sani.id = workflowInstanceId
    sani
  }
  
  override def parseJsonStr(contentStr: String){
	  super.parseJsonStr(contentStr)
    val content = JsonMethods.parse(contentStr)
    import org.json4s._
    implicit val formats = DefaultFormats
    this.attachFiles = (content \ "attach-list" \ classOf[JString]).asInstanceOf[List[String]]
    this.code = (content \ "code").extract[String]
    this.paramLine = (content \ "param-line").extract[String]
  }
  
  override def assembleJsonStr(): String = {
    import org.json4s.jackson.JsonMethods._
	  import org.json4s.JsonDSL._
	  import com.kent.util.Util._
    val c1 = JsonMethods.parse(super.assembleJsonStr())
    //val c2 = JsonMethods.parse(s""" {"location":"${location}","content":"${Util.transformJsonStr(content)}"}""")
    val attStr = JsonMethods.compact(JsonMethods.render(attachFiles))
    val c2 = JsonMethods.parse(s""" {"code":${transJsonStr(code)},
                                     "attach-list":${attStr},
                                     "param-line":${transJsonStr(paramLine)}
                                }""")
    val c3 = c1.merge(c2)
    JsonMethods.pretty(JsonMethods.render(c3))
  }
}

object ScriptNode {
  def apply(name: String): ScriptNode = new ScriptNode(name)
  def apply(name:String, node: scala.xml.Node): ScriptNode = parseXmlNode(name, node)
  
  def parseXmlNode(name: String, xmlNode: scala.xml.Node): ScriptNode = {
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