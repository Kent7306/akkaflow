package com.kent.workflow.node

import java.util.Date
import com.kent.pub.DeepCloneable
import com.kent.pub.Daoable
import java.sql.Connection
import java.sql.ResultSet
import com.kent.workflow.controlnode._
import com.kent.workflow.actionnode._

abstract class NodeInfo(var name: String) extends DeepCloneable[NodeInfo] with Daoable[NodeInfo] with Serializable{
  import com.kent.workflow.node.NodeInfo.Status._
  var workflowName: String = _
  var desc: String = _
  /**
   * 由该节点信息创建属于某工作流实例的节点实例
   */
  def createInstance(workflowInstanceId: String): NodeInstance
  def deepClone(): NodeInfo
  override def deepCloneAssist(e: NodeInfo): NodeInfo = {
    e.workflowName = workflowName
    e.desc = desc
    e
	}
  /**
   * merge
   */
  def save(implicit conn: Connection): Boolean = {
	  import com.kent.util.Util._
	  val isAction = if(this.isInstanceOf[ActionNodeInfo]) 1 else 0
	  val insertStr = s"""
      insert into node 
      values(${withQuate(name)},${isAction},${withQuate(this.getClass.getName)},
      ${withQuate(getContent())},${withQuate(workflowName)},${withQuate(desc)})
      """
    val updateStr = s"""
      update node set type = ${withQuate(this.getClass.getName)},
                      is_action = ${isAction},
                      content = ${withQuate(getContent())}, 
                      workflow_name = ${withQuate(workflowName)},
                      description = ${withQuate(desc)})
                      where name = ${withQuate(name)}
      """
    if(this.getEntity.isEmpty){
    	executeSql(insertStr)     
    }else{
      executeSql(updateStr)
    }
  }
  
  /**
   * 删除
   */
  def delete(implicit conn: Connection): Boolean = {
    import com.kent.util.Util._
    executeSql(s"delete from node where name = ${withQuate(name)} and workflow_name = ${withQuate(workflowName)}")
  }
    /**
   * 获取对象
   */
  def getEntity(implicit conn: Connection): Option[NodeInfo] = {
    import com.kent.util.Util._
    val newNode = this.deepClone()
    val queryStr = s"""
     select name,type,is_action,workflow_name,description,content
     from node 
     where name=${withQuate(name)} and workflow_name = ${withQuate(workflowName)}
                    """
    querySql(queryStr, (rs: ResultSet) =>{
          if(rs.next()){
            newNode.desc = rs.getString("description")
            newNode.workflowName = rs.getString("workflow_name")
            newNode.setContent(rs.getString("content"))
            newNode
          }else{
            null
            
          }
      })
  }
}

object NodeInfo {
  def apply(node: scala.xml.Node): NodeInfo = parseXmlNode(node)
  /**
   * 解析xml得到节点信息
   */
  def parseXmlNode(node: scala.xml.Node): NodeInfo = {
    node match {
      case <action>{content @ _*}</action> => ActionNodeInfo(node)
      case _ => ControlNodeInfo(node)
    }
  }
  
  def apply(nodeType: String, name: String, workflowName: String): NodeInfo = {
    val withDollar = nodeType + "$"
    
    val node = if(withDollar == StartNodeInfo.getClass.getName.replaceAll("$", "")) StartNodeInfo(name)
    else if(withDollar == EndNodeInfo.getClass.getName) EndNodeInfo(name)
    else if(withDollar == JoinNodeInfo.getClass.getName) JoinNodeInfo(name)
    else if(withDollar == KillNodeInfo.getClass.getName) KillNodeInfo(name)
    else if(withDollar == ForkNodeInfo.getClass.getName) ForkNodeInfo(name)
    else if(withDollar == ShellActionNodeInfo.getClass.getName) ShellActionNodeInfo(name)
    else if(withDollar == ScriptActionNodeInfo.getClass.getName) ScriptActionNodeInfo(name)
    else if(withDollar == FileWatcherActionNodeInfo.getClass.getName) FileWatcherActionNodeInfo(name)
    else null
    if(node != null)node.workflowName = workflowName
    node
  }
  
  object Status extends Enumeration { 
    type Status = Value
    val PREP, RUNNING, SUSPENDED, SUCCESSED,FAILED,KILLED = Value
    def getStatusWithId(id: Int): Status = {
      var sta: Status = PREP  
      Status.values.foreach { x => if(x.id == id) return x }
      sta
    }
    
  }
  object NodeTagType  extends Enumeration{
    type  NodeTagType = Value
    val KILL = Value("kill")
    val START = Value("start")
    val END = Value("end")
    val JOIN = Value("join")
    val FORK = Value("fork")
    val HOST_SCRIPT = Value("host_cript")
    val SUB_WORKFLOW = Value("sub_workflow")
    val ACTION = Value("action")
  }
}