package com.kent.workflow.node

import java.util.Date
import com.kent.pub.DeepCloneable
import com.kent.pub.Daoable
import java.sql.Connection
import java.sql.ResultSet

abstract class NodeInfo(var name: String) extends DeepCloneable[NodeInfo] with Daoable[NodeInfo] with Serializable{
  import com.kent.workflow.node.NodeInfo.Status._
  var workflowId: String = _
  var desc: String = _
  /**
   * 由该节点信息创建属于某工作流实例的节点实例
   */
  def createInstance(workflowInstanceId: String): NodeInstance
  def deepClone(): NodeInfo
  override def deepCloneAssist(e: NodeInfo): NodeInfo = {
    e.workflowId = workflowId
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
      ${withQuate(getContent())},${withQuate(workflowId)},${withQuate(desc)})
      """
    val updateStr = s"""
      update node set type = ${withQuate(this.getClass.getName)},
                      is_action = ${isAction},
                      content = ${withQuate(getContent())}, 
                      workflow_id = ${withQuate(workflowId)},
                      description = ${withQuate(desc)})
                      where name = ${withQuate(name)}
      """
     
    if(this.getEntity.isEmpty){
println(insertStr)
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
    executeSql(s"delete from node where name = ${withQuate(name)} and workflow_id = ${withQuate(workflowId)}")
  }
    /**
   * 获取对象
   */
  def getEntity(implicit conn: Connection): Option[NodeInfo] = {
    import com.kent.util.Util._
    val newNode = this.deepClone()
    val queryStr = s"""
     select name,type,is_action,workflow_id,description,content
     from node 
     where name=${withQuate(name)} and workflow_id = ${withQuate(workflowId)}
                    """
    querySql(queryStr, (rs: ResultSet) =>{
          if(rs.next()){
            newNode.desc = rs.getString("description")
            newNode.workflowId = rs.getString("workflow_id")
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