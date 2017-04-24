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
import com.kent.db.PersistManager.Save
import com.kent.workflow.controlnode._
import com.kent.workflow.actionnode._
import com.kent.workflow.controlnode._
import com.kent.pub.ShareData

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
    this.status = RUNNING 
    ShareData.persistManager ! Save(this.deepClone) 
    true
  }
  /**
   * 节点执行动作
   */
  def execute(): Boolean
  /**
   * 节点实例执行结束
   */
  def terminate(wfa: WorkflowActor): Boolean
  /**
   * 执行结束后回调方法
   */
  def postTerminate():Boolean = {
    ShareData.persistManager ! Save(this.deepClone())
    true
  }
  /**
   * 该节点是否满足执行条件
   */
  def ifCanExecuted(wfi: WorkflowInstance): Boolean = true
  /**
   * 得到该节点的下一执行节点集合
   */
  def getNextNodes(wfi: WorkflowInstance): List[NodeInstance]
  /**
   * 替换该节点参数
   */
  def replaceParam(param: Map[String, String]): Boolean
  
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
    values(${withQuate(id)},${withQuate(nodeInfo.name)},${isAction},${withQuate(this.getClass.getName)},
          ${withQuate(getContent())},${withQuate(nodeInfo.desc)},
           ${status.id},${withQuate(formatStandarTime(startTime))},
           ${withQuate(formatStandarTime(endTime))},${withQuate(executedMsg)})
    """
    val updateStr = s"""
      update node_instance set name = ${withQuate(nodeInfo.name)},
                               is_action = ${isAction},
                               type = ${withQuate(this.getClass.getName)},
                               content = ${withQuate(getContent())},
                               description = ${withQuate(nodeInfo.desc)},
                               status = ${status.id},
                               stime = ${withQuate(formatStandarTime(startTime))},
                               etime = ${withQuate(formatStandarTime(endTime))},
                               msg = ${withQuate(executedMsg)}
                      where name = ${withQuate(nodeInfo.name)} and workflow_instance_id = ${withQuate(id)}
      """
    if(this.getEntity.isEmpty){
    	executeSql(insertStr) 
    }else{
      executeSql(updateStr)
    }
  }
  /**
   * 获取对象
   */
  def getEntity(implicit conn: Connection): Option[NodeInstance] = {
    import com.kent.util.Util._
    val newNodeInstance = this.deepClone
    val queryStr = s"""
         select workflow_instance_id,name,is_action,type,content,description,status,stime,etime,msg
         from node_instance 
         where name = ${withQuate(nodeInfo.name)} and workflow_instance_id = ${withQuate(id)}
                    """
   querySql(queryStr, (rs: ResultSet) =>{
          import com.kent.workflow.node.NodeInfo.Status
          if(rs.next()){
        	   newNodeInstance.nodeInfo.name = rs.getString("name")
        	   newNodeInstance.nodeInfo.desc = rs.getString("description")
        	   newNodeInstance.status = Status.getStatusWithId(rs.getInt("status")) 
        	   newNodeInstance.executedMsg = rs.getString("msg")
             newNodeInstance.startTime = Util.getStandarTimeWithStr(rs.getString("stime"))
             newNodeInstance.endTime = Util.getStandarTimeWithStr(rs.getString("etime"))
             newNodeInstance.setContent(rs.getString("content"))
             newNodeInstance
          }else{
            null
          }
      })
  }
}

object NodeInstance {
  def apply(nodeType: String, name: String, id: String): NodeInstance = {
    val withDollar = nodeType + "$"
    if(withDollar == StartNodeInstance.getClass.getName.replaceAll("$", "")) StartNodeInfo(name).createInstance(id)
    else if(withDollar == EndNodeInstance.getClass.getName) EndNodeInfo(name).createInstance(id)
    else if(withDollar == JoinNodeInstance.getClass.getName) JoinNodeInfo(name).createInstance(id)
    else if(withDollar == KillNodeInstance.getClass.getName) KillNodeInfo(name).createInstance(id)
    else if(withDollar == ForkNodeInstance.getClass.getName) ForkNodeInfo(name).createInstance(id)
    else if(withDollar == ShellActionNodeInstance.getClass.getName) ShellActionNodeInfo(name).createInstance(id)
    else if(withDollar == ScriptActionNodeInstance.getClass.getName) ScriptActionNodeInfo(name).createInstance(id)
    else if(withDollar == FileWatcherActionNodeInstance.getClass.getName) FileWatcherActionNodeInfo(name).createInstance(id)
    else null
  }
  
}

