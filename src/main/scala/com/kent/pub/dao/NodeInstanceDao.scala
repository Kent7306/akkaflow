package com.kent.pub.dao

import com.kent.daemon.DbConnector
import com.kent.util.Util
import com.kent.util.Util.wq
import com.kent.workflow.node.Node.Status
import com.kent.workflow.node.{Node, NodeInstance}
import com.kent.workflow.node.action.ActionNodeInstance

/**
  * @author kent
  * @date 2019-04-20
  * @desc 实例节点数据操作对象
  *
  **/
object NodeInstanceDao {
  /**
    * 删除
    */
  def delete(ni: NodeInstance): Boolean = {
    val sql = s"delete from node_instance where name = '${ni.nodeInfo.name}' and workflow_instance_id = '${ni.id}'"
    DbConnector.executeSyn(sql)
  }

  /**
    * merge实例节点
    *
    * @param ni
    * @return
    */
  def merge(ni: NodeInstance): Boolean = {
    import com.kent.util.Util._
    val isAction = if (ni.isInstanceOf[ActionNodeInstance]) 1 else 0
    val insertStr =
      s"""
    insert into node_instance
    values(${wq(ni.id)},${wq(ni.nodeInfo.name)},${isAction},${wq(ni.nodeInfo.getClass.getName.split("\\.").last)},
          ${wq(ni.nodeInfo.toJson())},${wq(ni.nodeInfo.desc)},
           ${ni.status.id},${wq(formatStandarTime(ni.startTime))},
           ${wq(formatStandarTime(ni.endTime))},${wq(escapeStr(ni.executedMsg))})
    """
    val delStr = s"delete from node_instance where name = ${wq(ni.nodeInfo.name)} and workflow_instance_id = ${wq(ni.id)}"
    val sqls = delStr +: insertStr +: Nil
    DbConnector.executeSyn(sqls)

  }

  /**
    * 更新实例节点
    *
    * @param ni
    * @return
    */
  def update(ni: NodeInstance): Boolean = {
    import com.kent.util.Util._
    val updateStr =
      s"""
      update node_instance set status = ${ni.status.id},
                               stime = ${wq(formatStandarTime(ni.startTime))},
                               etime = ${wq(formatStandarTime(ni.endTime))},
                               msg = ${wq(escapeStr(ni.executedMsg))}
                      where name = ${wq(ni.nodeInfo.name)} and workflow_instance_id = ${wq(ni.id)}
      """
    DbConnector.executeSyn(updateStr)
  }

  def get(node: Node, wfiId: String): Option[NodeInstance] = {
    val queryNodesStr =
      s"""
         select * from node_instance where workflow_instance_id = ${wq(wfiId)} and name = ${wq(node.name)}
      """
    DbConnector.querySyn[NodeInstance](queryNodesStr, rs => {
      if (rs.next()) {
        val executeMsg = rs.getString("msg")
        val startTime = Util.getStandarTimeWithStr(rs.getString("stime"))
        val endTime = Util.getStandarTimeWithStr(rs.getString("etime"))
        val status = Status.getStatusWithId(rs.getInt("status"))
        val ni = node.createInstance(wfiId)
        ni.executedMsg = executeMsg
        ni.startTime = startTime
        ni.endTime = endTime
        ni.status = status
        ni
      } else {
        null
      }
    })
  }
}
