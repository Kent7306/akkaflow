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
import org.json4s.jackson.JsonMethods

abstract class NodeInfo(var name: String) extends Daoable[NodeInfo] with DeepCloneable[NodeInfo] {
  import com.kent.workflow.node.NodeInfo.Status._
  var workflowName: String = _
  var desc: String = _
  /**
   * 获取对象的json
   */
  def getCateJson(): String
  def getJson(): String
  /**
   * 组装对象的content内容
   */
  def assembleJson(): String = {
    val cateJson = getCateJson()
    val nodeJson = getJson()
    val c1 = JsonMethods.parse(cateJson)
    val c2 = JsonMethods.parse(nodeJson)
    val c3 = c1.merge(c2)
    JsonMethods.pretty(JsonMethods.render(c3))
  }
  /**
   * 由该节点信息创建属于某工作流实例的节点实例
   */
   def createInstance(workflowInstanceId: String):NodeInstance = {
    val nodeInstanceClass = Class.forName(this.getClass.getName+"Instance")
    val constr = nodeInstanceClass.getConstructor(this.getClass)
    type nodeType = this.type
    val node = this.asInstanceOf[nodeType]
    val ni = constr.newInstance(node).asInstanceOf[NodeInstance]
    ni.id = workflowInstanceId
    ni
  }
  
  /**
   * merge
   */
  def save(implicit conn: Connection): Boolean = {
    import com.kent.util.Util._
    val isAction = if (this.isInstanceOf[ActionNode]) 1 else 0
    val insertStr = s"""
      insert into node 
      values(${withQuate(name)},${isAction},${withQuate(this.getClass.getName.split("\\.").last)},
      ${withQuate(assembleJson())},${withQuate(workflowName)},${withQuate(desc)})
      """
      executeSql(insertStr)
  }

  /**
   * 删除
   */
  def delete(implicit conn: Connection): Boolean = {
    import com.kent.util.Util._
    executeSql(s"delete from node where name = ${withQuate(name)} and workflow_name = ${withQuate(workflowName)}")
  }
  /**
   * 获取对象（不用实现）
   */
  def getEntity(implicit conn: Connection): Option[NodeInfo] = ???
}

object NodeInfo {
  def apply(node: scala.xml.Node): NodeInfo = {
    node match {
      case <action>{ content @ _* }</action> => ActionNode(node)
      case _                                 => ControlNode(node)
    }
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