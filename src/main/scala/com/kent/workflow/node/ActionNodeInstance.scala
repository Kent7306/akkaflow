package com.kent.workflow.node

import com.kent.workflow.WorkflowInstance
import com.kent.workflow.node.NodeInfo.Status._
import com.kent.workflow.WorkflowInfo.WStatus._
import com.kent.workflow.WorkflowActor
import com.kent.util.Util
import java.util.Calendar
import java.util.Date
import org.json4s.jackson.JsonMethods
import com.kent.workflow.ActionActor

abstract class ActionNodeInstance(override val nodeInfo: ActionNodeInfo) extends NodeInstance(nodeInfo) {
  var hasRetryTimes: Int = 0
  var allocateHost: String = _
  var actionActor: ActionActor = _
  def kill():Boolean
  
  /**
   * 得到下一个节点
   */
  override def getNextNodes(wfi: WorkflowInstance): List[NodeInstance] = {
    status match {
          case SUCCESSED => wfi.nodeInstanceList.filter { _.nodeInfo.name == this.nodeInfo.ok }.toList
          case FAILED => wfi.nodeInstanceList.filter { _.nodeInfo.name == this.nodeInfo.error }.toList
          case KILLED => wfi.nodeInstanceList.filter { _.nodeInfo.name == this.nodeInfo.error }.toList
          case _ => throw new Exception(s"[workflow:${wfi.workflow.name}]的[action:${this.nodeInfo.name}]执行状态出错")
        }
  }

  override def run(wfa: WorkflowActor): Boolean = {
    this.preExecute()
    wfa.createAndStartActionActor(this)
  }
  /**
   * 找到下一执行节点
   */
  def terminate(wfa: WorkflowActor): Boolean = {
      this.endTime = Util.nowDate
      this.status match {
      case SUCCESSED => 
      case FAILED => 
        if(this.getNextNodes(wfa.workflowInstance).size <=0){    //若该action节点执行失败后无下一节点
          wfa.fail()
      		return false
        }
      case KILLED =>
        wfa.kill()
        return false
    }
    //查找下一节点
    val nodes = this.getNextNodes(wfa.workflowInstance)
    nodes.filter { _.ifCanExecuted(wfa.workflowInstance) }.foreach { x => wfa.waitingNodes = wfa.waitingNodes.enqueue(x)}
    return true
  }
    
  override def setContent(contentStr: String){
    val content = JsonMethods.parse(contentStr)
    import org.json4s._
    implicit val formats = DefaultFormats
    this.nodeInfo.setContent(contentStr)
    this.hasRetryTimes = (content \ "has-retry-times").extract[Int]
  }
  override def getContent(): String = {
    val ncontent = this.nodeInfo.getContent()
    val c1 = JsonMethods.parse(ncontent)
    val c2 = JsonMethods.parse(s"""{"has-retry-times":${hasRetryTimes}}""")
    val c3 = c1.merge(c2)
    JsonMethods.pretty(JsonMethods.render(c3))
  }
  
  override def toString(): String = {
	    var str = "  "+this.getClass.getName + "(\n"
	    str = str + s"    id = ${id},\n"
	    str = str + s"    name = ${nodeInfo.name},\n"
	    str = str + s"    status = ${status},\n"
	    str = str + s"    startTime = ${startTime},\n"
	    str = str + s"    endTime = ${endTime})\n"
	    str = str + s"    executedMsg = ${executedMsg}\n"
	    str = str + s"    hasRetryTimes = ${hasRetryTimes}\n"
	    str
	  }
}