package com.kent.workflow.node

import com.kent.workflow.WorkflowInstance
import com.kent.workflow.WorkflowActor
import com.kent.util.Util
import com.kent.workflow.node.NodeInfo.Status._
import org.json4s.jackson.JsonMethods

abstract class ControlNodeInstance(override val nodeInfo: ControlNodeInfo) extends NodeInstance(nodeInfo){
  def execute(): Boolean = true
  def terminate(wfa: WorkflowActor): Boolean = {
    this.endTime = Util.nowDate
	  this.status = SUCCESSED
	  //查找下一节点
	  wfa.getNextNodesToWaittingQueue(this)
	  true
  }
  
  override def toString(): String = {
	    var str = "  "+this.getClass.getName + "(\n"
	    str = str + s"    id = ${id},\n"
	    str = str + s"    name = ${nodeInfo.name},\n"
	    str = str + s"    status = ${status},\n"
	    str = str + s"    startTime = ${startTime},\n"
	    str = str + s"    endTime = ${endTime})\n"
	    str
	  }

  def replaceParam(param: Map[String, String]): Boolean = true
  
  override def parseJsonStr(contentStr: String){
    if(contentStr != null){
    	val content = JsonMethods.parse(contentStr)
			import org.json4s._
			implicit val formats = DefaultFormats
			this.nodeInfo.parseJsonStr(contentStr)       
    }
  }
  override def assembleJsonStr(): String = {
    val ncontent = this.nodeInfo.assembleJsonStr()
    if(ncontent != null){
    	val c1 = JsonMethods.parse(ncontent)
    	JsonMethods.pretty(JsonMethods.render(c1))      
    }else {
      null
    }
  }
}