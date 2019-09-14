package com.kent.workflow.node

import java.util.Date

import com.kent.pub.DeepCloneable
import com.kent.pub.dao.NodeInstanceDao
import com.kent.util.Util
import com.kent.workflow.{WorkflowActor, WorkflowInstance}
import com.kent.workflow.node.Node.Status._

abstract class NodeInstance(val nodeInfo: Node) extends DeepCloneable[NodeInstance]{
  var id: String = _
  @volatile var status: Status = PREP
  @volatile var executedMsg: String = _
  var startTime: Date= _
  var endTime: Date = _
  
  def name = s"${id}_${nodeInfo.name}"
  
  /**
   * 节点实例运行入口
   */
  def run(wfa: WorkflowActor):Boolean = {
    this.preExecute()
    this.execute()
		this.terminate(wfa)
		this.postTerminate()
  }
  /**
   * 执行前方法
   */
  def preExecute():Boolean = {
    this.startTime = Util.nowDate 
    this.changeStatus(RUNNING) 
    true
  }
  /**
   * 节点执行动作
   */
  @throws(classOf[Exception])
  def execute(): Boolean
  /**
   * 节点实例执行结束（若有下一个节点则加入到节点等待队列，若无则结束）
   */
  def terminate(wfa: WorkflowActor): Boolean
  /**
   * 执行结束后回调方法
   */
  def postTerminate():Boolean = true
  /**
   * 该节点是否满足执行条件
   */
  def ifCanExecuted(wfi: WorkflowInstance): Boolean = true
  /**
   * 得到该节点的下一执行节点集合
   */
  def getNextNodes(wfi: WorkflowInstance): List[NodeInstance]
  /**
   * 重置
   */
  def reset(){
    this.endTime = null
    this.status = PREP
    this.executedMsg = null
    this.startTime = null
  }
  /**
   * 更改节点实例状态（需要保存）
   */
  def changeStatus(status: Status){
    this.status = status
    NodeInstanceDao.update(this)
  }
  def getStatus():Status = this.status
}

object NodeInstance {
  def apply(nodeType: String, name: String, id: String): NodeInstance = {
    val nodeClass = Class.forName(nodeType)
    val method = nodeClass.getMethod("apply", "str".getClass)
    val node = method.invoke(null, name).asInstanceOf[Node];
    val ni = node.createInstance(id)
    ni
  }
  
}

