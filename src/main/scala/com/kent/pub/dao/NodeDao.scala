package com.kent.pub.dao

import com.kent.util.Util.wq
import com.kent.workflow.node._
import com.kent.workflow.node.action.ActionNode

/**
  * @author kent
  * @date 2019-08-20
  * @desc 节点数据库操作对象
  *
  **/
object NodeDao extends TransationManager with Daoable{

  def merge(node: Node): Boolean = {
    if (isExists(node)){
      update(node)
    } else {
      save(node)
    }
  }


  def save(node: Node): Boolean = {
    val isAction = if (node.isInstanceOf[ActionNode]) 1 else 0
    val insertSql = s"""
      insert into node
      values(${wq(node.name)},$isAction,${wq(node.getClass.getName.split("\\.").last)},
      ${wq(node.toJson())},${wq(node.workflowName)},${wq(node.desc)})
      """
    transaction{
      execute(insertSql)
    }
  }

  def isExists(node: Node): Boolean = {
    transaction{
      val sql = s"""select count(1) as cnt from node where name=${wq(node.name)} and workflow_name=${wq(node.workflowName)}"""
      query[Boolean](sql, rs => {
        val cnt = if (rs.next()) rs.getInt("cnt") else 0
        if (cnt > 0) true else false
      }).get
    }
  }

  def update(node: Node): Boolean = {
    val isAction = if (node.isInstanceOf[ActionNode]) 1 else 0
    val updateSql =
      s"""
         |update node set is_action = $isAction,
         |                 type = ${wq(node.getClass.getName.split("\\.").last)},
         |                 content = ${wq(node.toJson())},
         |                 description = ${wq(node.desc)}
         |where name = ${wq(node.name)} and workflow_name = ${wq(node.workflowName)}
       """.stripMargin
    transaction{
      execute(updateSql)
    }
  }

  def delete(node: Node): Boolean = {
    val delSql =
      s"""
        | delete from node where name = ${wq(node.name)} and ${wq(node.workflowName)}
      """.stripMargin

    transaction{
      execute(delSql)
    }
  }
}
