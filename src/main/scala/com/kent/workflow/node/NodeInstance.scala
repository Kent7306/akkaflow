package com.kent.workflow.node

import com.kent.workflow.node.NodeInfo.Status._
import com.kent.workflow.WorkflowActor
import com.kent.workflow.WorkflowInstance
import java.util.Date
import com.kent.pub.DeepCloneable
import com.kent.pub.Daoable
import java.sql.Connection
import com.kent.util.Util
import java.sql.ResultSet
import com.kent.db.PersistManager
import com.kent.pub.Event._
import com.kent.workflow.controlnode._
import com.kent.workflow.actionnode._
import com.kent.workflow.controlnode._
import com.kent.main.Master
import scala.concurrent.Future

abstract class NodeInstance(val nodeInfo: NodeInfo) extends Daoable[NodeInstance] with DeepCloneable[NodeInstance]{
  var id: String = _
  var status: Status = PREP
  var executedMsg: String = _
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
    if(Master.persistManager != null) Master.persistManager ! Save(this.deepClone) 
  }
  def getStatus():Status = this.status
  /**
   * 删除
   */
  def delete(implicit conn: Connection): Boolean = {
    executeSql(s"delete from node_instance where name = '${nodeInfo.name}' and workflow_instance_id = '${id}'")
  }
  
  /**
   * merge
   */
  def save(implicit conn: Connection): Boolean = {
	  import com.kent.util.Util._
    val isAction = if(this.isInstanceOf[ActionNodeInstance]) 1 else 0
    val insertStr = s"""
    insert into node_instance
    values(${withQuate(id)},${withQuate(nodeInfo.name)},${isAction},${withQuate(this.nodeInfo.getClass.getName.split("\\.").last)},
          ${withQuate(nodeInfo.assembleJson())},${withQuate(nodeInfo.desc)},
           ${status.id},${withQuate(formatStandarTime(startTime))},
           ${withQuate(formatStandarTime(endTime))},${withQuate(executedMsg)})
    """
    val updateStr = s"""
      update node_instance set status = ${status.id},
                               stime = ${withQuate(formatStandarTime(startTime))},
                               etime = ${withQuate(formatStandarTime(endTime))},
                               msg = ${withQuate(executedMsg)}
                      where name = ${withQuate(nodeInfo.name)} and workflow_instance_id = ${withQuate(id)}
      """
    val isExistsql = s"select name from node_instance where name = ${withQuate(nodeInfo.name)} and workflow_instance_id = ${withQuate(id)}"
    querySql[Boolean](isExistsql, (rs) => {
      if(rs.next()) executeSql(updateStr) else executeSql(insertStr)
    }).get
  }
  /**
   * 获取对象(不用实现)
   */
  def getEntity(implicit conn: Connection): Option[NodeInstance] = ???
}

object NodeInstance {
  def apply(nodeType: String, name: String, id: String): NodeInstance = {
    val nodeClass = Class.forName(nodeType)
    val method = nodeClass.getMethod("apply", "str".getClass)
    val node = method.invoke(null, name).asInstanceOf[NodeInfo];
    val ni = node.createInstance(id)
    ni
  }
  
}

