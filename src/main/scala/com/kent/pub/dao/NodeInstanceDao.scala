package com.kent.pub.dao

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
object NodeInstanceDao extends TransationManager with Daoable {
  /**
    * 删除
    */
  def delete(ni: NodeInstance): Boolean = {
    transaction {
      val sql = s"delete from node_instance where name = '${ni.nodeInfo.name}' and workflow_instance_id = '${ni.id}'"
      execute(sql)
    }
  }

  /**
    * 删除某个实例下的所有节点实例
    * @param wfiId
    * @return
    */
  def deleteNodesWithInstanceId(wfiId: String): Boolean = {
    transaction {
      val sql = s"delete from node_instance where workflow_instance_id = '${wfiId}'"
      execute(sql)
    }
  }


  def save(ni: NodeInstance): Boolean = {
    import com.kent.util.Util._
    transaction {
      val isAction = if (ni.isInstanceOf[ActionNodeInstance]) 1 else 0
      val insertStr =
        s"""
    insert into node_instance
    values(${wq(ni.id)},${wq(ni.nodeInfo.name)},$isAction,
           ${wq(ni.nodeInfo.getClass.getName.split("\\.").last)},
           ${wq(ni.nodeInfo.toJson())},${wq(ni.nodeInfo.desc)},
           ${ni.status.id},${wq(formatStandardTime(ni.startTime))},
           ${wq(formatStandardTime(ni.endTime))},${wq(escapeStr(ni.executedMsg))})
    """
      execute(insertStr)
    }
  }

  /**
    * merge实例节点
    *
    * @param ni
    * @return
    */
  def merge(ni: NodeInstance): Boolean = {
    transaction {
      this.delete(ni)
      this.save(ni)
    }
  }

  /**
    * 更新实例节点
    *
    * @param ni
    * @return
    */
  def update(ni: NodeInstance): Boolean = {
    import com.kent.util.Util._
    transaction {
      //内容过长进行截取
      val newMsg = if (ni.executedMsg != null && ni.executedMsg.getBytes.length >= 1024){
        val byteLen = ni.executedMsg.getBytes("UTF-8")
        val bytes = byteLen.take(1010)
        val contentTmp = new String(bytes, "UTF-8")
        contentTmp.substring(0, contentTmp.length - 3)+"..."
      } else {
        ni.executedMsg
      }
      val updateStr =
        s"""
      update node_instance set status = ${ni.status.id},
                               stime = ${wq(formatStandardTime(ni.startTime))},
                               etime = ${wq(formatStandardTime(ni.endTime))},
                               msg = ${wq(escapeStr(newMsg))}
                      where name = ${wq(ni.nodeInfo.name)} and workflow_instance_id = ${wq(ni.id)}
      """
      execute(updateStr)
    }
  }

  def get(node: Node, wfiId: String): Option[NodeInstance] = {
    transaction {
      val queryNodesStr =
        s"""
         select * from node_instance where workflow_instance_id = ${wq(wfiId)} and name = ${wq(node.name)}
      """
      query[NodeInstance](queryNodesStr, rs => {
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
}
