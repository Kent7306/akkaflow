package com.kent.workflow.node

import java.sql.Connection

import com.kent.pub.{DeepCloneable, Persistable}
import com.kent.workflow.Workflow
import com.kent.workflow.node.action._
import com.kent.workflow.node.control._

abstract class Node(var name: String) extends Persistable[Node] with DeepCloneable[Node] {
  var workflowName: String = _
  var desc: String = _
  /**
   * 节点信息转换为json
   */
  def toJson(): String
  
  def getType = this.getClass.getName.split("\\.").last
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
   * 检查完整性，不通过直接抛出异常
   */
  def checkIntegrity(wf: Workflow) 
  
  /**
   * merge
   */
  def save(implicit conn: Connection): Boolean = {
    import com.kent.util.Util._
    val isAction = if (this.isInstanceOf[ActionNode]) 1 else 0
    val insertSql = s"""
      insert into node 
      values(${wq(name)},${isAction},${wq(this.getClass.getName.split("\\.").last)},
      ${wq(toJson())},${wq(workflowName)},${wq(desc)})
      """
    val updateSql = s"""
      update node set is_action = ${isAction},
                      type = ${wq(this.getClass.getName.split("\\.").last)},
                      content = ${wq(toJson())},
                      description = ${wq(desc)})
      where name = values(${wq(name)} and workflow_name = ${wq(workflowName)}
      """
      
    val isExistSql = s"select name from node where name = ${wq(name)} and workflow_name = ${wq(workflowName)}"
    val isExist = querySql(isExistSql, rs =>
      if(rs.next()) true else false
    )
    if(!isExist.get) executeSql(insertSql) else executeSql(updateSql)
  }

  /**
   * 删除
   */
  def delete(implicit conn: Connection): Boolean = {
    import com.kent.util.Util._
    executeSql(s"delete from node where name = ${wq(name)} and workflow_name = ${wq(workflowName)}")
  }
  /**
   * 获取对象（不用实现）
   */
  def getEntity(implicit conn: Connection): Option[Node] = ???
}

object Node {
  def apply(node: scala.xml.Node): Node = {
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
    def getStatusName(status: Status):String = {
      status match {
        case PREP => "就绪"
        case RUNNING => "运行中"
        case SUSPENDED => "挂起"
        case SUCCESSED => "成功"
        case FAILED => "失败"
        case KILLED => "杀死"
      }
    }
  }

}