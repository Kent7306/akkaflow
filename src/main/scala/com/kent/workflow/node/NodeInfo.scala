package com.kent.workflow.node

import java.util.Date
import com.kent.pub.DeepCloneable
import com.kent.pub.Daoable
import java.sql.Connection
import java.sql.ResultSet
import com.kent.workflow.controlnode._
import com.kent.workflow.actionnode._
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import java.io.ByteArrayInputStream
import java.io.ObjectInputStream
import java.io.IOException

abstract class NodeInfo(var name: String) extends Daoable[NodeInfo] with DeepCloneable[NodeInfo] {
  import com.kent.workflow.node.NodeInfo.Status._
  var workflowName: String = _
  var desc: String = _
  /**
   * 由该节点信息创建属于某工作流实例的节点实例
   */
  def createInstance(workflowInstanceId: String): NodeInstance

  /**
   * merge
   */
  def save(implicit conn: Connection): Boolean = {
    import com.kent.util.Util._
    val isAction = if (this.isInstanceOf[ActionNodeInfo]) 1 else 0
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
    if (this.getEntity.isEmpty) {
      executeSql(insertStr)
    } else {
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
    val newNode = this.deepClone
    val queryStr = s"""
     select name,type,is_action,workflow_name,description,content
     from node 
     where name=${withQuate(name)} and workflow_name = ${withQuate(workflowName)}
                    """
    querySql(queryStr, (rs: ResultSet) => {
      if (rs.next()) {
        newNode.desc = rs.getString("description")
        newNode.workflowName = rs.getString("workflow_name")
        newNode.setContent(rs.getString("content"))
        newNode
      } else {
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
      case <action>{ content @ _* }</action> => ActionNodeInfo(node)
      case _                                 => ControlNodeInfo(node)
    }
  }

  def apply(nodeType: String, name: String, workflowName: String): NodeInfo = {
    //这里用了反射
    val nodeClass = Class.forName(nodeType)
    val method = nodeClass.getMethod("apply", "str".getClass)
    val node = method.invoke(null, name).asInstanceOf[NodeInfo];
    if (node != null) node.workflowName = workflowName
    node
  }

  object Status extends Enumeration {
    type Status = Value
    val PREP, RUNNING, SUSPENDED, SUCCESSED, FAILED, KILLED = Value
    def getStatusWithId(id: Int): Status = {
      var sta: Status = PREP
      Status.values.foreach { x => if (x.id == id) return x }
      sta
    }
  }

}