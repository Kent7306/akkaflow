package com.kent.workflow.node

import java.sql.Connection

import com.kent.pub.{DeepCloneable, Persistable}
import com.kent.workflow.Workflow
import com.kent.workflow.node.action._
import com.kent.workflow.node.control._

abstract class Node(var name: String) extends DeepCloneable[Node] {
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