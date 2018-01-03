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
import com.kent.db.LogRecorder
import com.kent.db.LogRecorder.LogType
import com.kent.db.LogRecorder.LogType._

abstract class ActionNodeInstance(override val nodeInfo: ActionNode) extends NodeInstance(nodeInfo) {
  var hasRetryTimes: Int = 0
  var allocateHost: String = _
  var actionActor: ActionActor = _
  
  //进行日志截断
  private val logLimiteNum: Int = 1000
  private var logIdx: Int = 0
  
  
  def kill():Boolean
  
  /**
   * 得到下一个节点
   */
  override def getNextNodes(wfi: WorkflowInstance): List[NodeInstance] = {
    this.getStatus() match {
          case SUCCESSED => wfi.nodeInstanceList.filter { _.nodeInfo.name == this.nodeInfo.ok }.toList
          case FAILED => wfi.nodeInstanceList.filter { _.nodeInfo.name == this.nodeInfo.error }.toList
          case KILLED => wfi.nodeInstanceList.filter { _.nodeInfo.name == this.nodeInfo.error }.toList
          case _ => throw new Exception(s"[workflow:${wfi.workflow.name}]的[action:${this.nodeInfo.name}]执行状态出错")
        }
  }

  override def run(wfa: WorkflowActor): Boolean = {
    this.preExecute()
    wfa.createAndStartActionActor(this)
    true
  }
  /**
   * 找到下一执行节点
   */
  def terminate(wfa: WorkflowActor): Boolean = {
      this.getStatus() match {
      case SUCCESSED => 
      case FAILED => 
        if(this.getNextNodes(wfa.workflowInstance).size <=0){    //若该action节点执行失败后无下一节点
          wfa.terminateWith(W_FAILED, "工作流实例执行失败")
      		return false
        }
      case KILLED =>
        wfa.terminateWith(W_KILLED, "工作流实例被杀死")
        return false
    }
    //查找下一节点
    wfa.getNextNodesToWaittingQueue(this)
    return true
  }
  /**
   * INFO日志级别，超出则用以截断日志
   */
  def infoLog(line: String) = {
    logIdx += 1
    if(logIdx < logLimiteNum) LogRecorder.info(ACTION_NODE_INSTANCE, this.id, this.nodeInfo.name, line)
    else if(logIdx == logLimiteNum) errorLog(line)
  }
  /**
   * ERROR日志级别
   */
  def errorLog(line: String) = LogRecorder.error(ACTION_NODE_INSTANCE, this.id, this.nodeInfo.name, line) 
  /**
   * WARN日志级别
   */
  def warnLog(line: String) = LogRecorder.warn(ACTION_NODE_INSTANCE, this.id, this.nodeInfo.name, line) 
  
  override def toString(): String = {
	    var str = "  "+this.getClass.getName + "(\n"
	    str = str + s"    id = ${id},\n"
	    str = str + s"    name = ${nodeInfo.name},\n"
	    str = str + s"    status = ${this.getStatus()},\n"
	    str = str + s"    startTime = ${startTime},\n"
	    str = str + s"    endTime = ${endTime})\n"
	    str = str + s"    executedMsg = ${executedMsg}\n"
	    str = str + s"    hasRetryTimes = ${hasRetryTimes}\n"
	    str
	  }
}