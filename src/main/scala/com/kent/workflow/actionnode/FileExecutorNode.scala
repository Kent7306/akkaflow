package com.kent.workflow.actionnode

import com.kent.workflow.node.ActionNodeInfo
import com.kent.workflow.node.NodeInstance
import com.kent.workflow.node.NodeInfo
import org.json4s.jackson.JsonMethods
import com.kent.util.Util
import java.io.File

class FileExecutorNode(name: String) extends ActionNodeInfo(name) {
  var attachFiles = List[String]()
  var command: String = _
  
  def createInstance(workflowInstanceId: String): FileExecutorNodeInstance = {
    val sani = FileExecutorNodeInstance(this.deepCloneAs[FileExecutorNode]) 
    sani.id = workflowInstanceId
    sani
  }
  
  override def setContent(contentStr: String){
	  super.setContent(contentStr)
    val content = JsonMethods.parse(contentStr)
    import org.json4s._
    implicit val formats = DefaultFormats
    this.command = (content \ "command").extract[String]
    this.attachFiles = (content \ "attach-list" \ classOf[JString]).asInstanceOf[List[String]]
  }
  
  override def getContent(): String = {
		import org.json4s.jackson.JsonMethods._
	  import org.json4s.JsonDSL._
    val c1 = JsonMethods.parse(super.getContent())
    val attStr = JsonMethods.compact(JsonMethods.render(attachFiles))
    val c2 = JsonMethods.parse(s""" {"command":"${Util.transformJsonStr(command)}","attach-list":${attStr}}""")
    val c3 = c1.merge(c2)
    JsonMethods.pretty(JsonMethods.render(c3))
  }
  /**
   * 从命令串中解析可执行文件
   */
  def analysisExecuteFilePath():String = {
    val strs = command.split("\\s+")
    //???
    val efs = strs.filter { x => x.contains("/") }
    if(efs.size != 1){
      throw new Exception("无法识别可执行文件")
    }else{
      return efs(0)
    }
  }
}

object FileExecutorNode {
  def apply(name: String): FileExecutorNode = new FileExecutorNode(name)
  def apply(name:String, xmlNode: scala.xml.Node): FileExecutorNode = parseXmlNode(name, xmlNode)
  
  def parseXmlNode(name: String, xmlNode: scala.xml.Node): FileExecutorNode = {
	  val node = FileExecutorNode(name)
	  //脚本执行命令
	  val commandSeq = (xmlNode \ "command")
	  if(commandSeq.size == 0){
	    throw new Exception(s"节点[file-executor: ${name}] 未配置<command>子标签")
	  }
	  node.command = commandSeq(0).text
	  //附加文件，可选
	  var attchFilesSeq = (xmlNode \ "attach-list")
	  node.attachFiles = (attchFilesSeq \ "file").map(f => f(0).text).toList
	  println(node.attachFiles+"*****")
	  node
  }
}