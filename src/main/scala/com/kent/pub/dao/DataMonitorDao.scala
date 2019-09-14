package com.kent.pub.dao


import com.kent.workflow.node.action.DataMonitorRecord
import com.kent.util.Util._

/**
  * @author kent
  * @date 2019-04-26
  * @desc 数据监测节点记录数据操作类
  *
  **/
object DataMonitorDao extends TransationManager with Daoable {
  def save(record: DataMonitorRecord): Boolean = {
    transaction {
      val minStr = if (record.minOpt.isDefined) record.minOpt.get.toString() else null
      val maxStr = if (record.maxOpt.isDefined) record.maxOpt.get.toString() else null
      val insertSql =
        s"""
  	     insert into data_monitor values(${wq(record.sdate)},${wq(record.category)},${wq(record.sourceName)},
  	     ${wq(record.num.toString())},${wq(minStr)},${wq(maxStr)},${wq(record.wfiId)},${wq(record.remark)})
  	    """
      execute(insertSql)
    }
  }


  def update(record: DataMonitorRecord): Boolean = {
    transaction {
      val minStr = if (record.minOpt.isDefined) record.minOpt.get.toString() else null
      val maxStr = if (record.maxOpt.isDefined) record.maxOpt.get.toString() else null
      val updateSql =
        s"""
  	    update data_monitor set num = ${wq(record.num.toString())},
  	                            min = ${wq(minStr)},
  	                            max = ${wq(maxStr)},
  	                            workflow_instance_id = ${wq(record.wfiId)},
  	                            remark = ${wq(record.remark)}
  	    where sdate=${wq(record.sdate)}
          and category=${wq(record.category)}
          and name=${wq(record.sourceName)}
  	  """
      execute(updateSql)
    }
  }

  def isExist(record: DataMonitorRecord): Boolean = {
    transaction {
      val sql =
        s"""
         select * from data_monitor
         where sdate=${wq(record.sdate)} and category=${wq(record.category)}
           and name=${wq(record.sourceName)}"""
      query[Boolean](sql, rs => {
        if (rs.next()) true else false
      }).get
    }
  }

  def delete(record: DataMonitorRecord): Boolean = {
    transaction {
      val sql =
        s"""
         delete from data_monitor
         where sdate=${wq(record.sdate)} and category=${wq(record.category)}
           and name=${wq(record.sourceName)}
       """
      execute(sql)
    }
  }
  def merge(record: DataMonitorRecord): Boolean = {
    transaction {
      val isExists = this.isExist(record)
      if (isExists) update(record) else save(record)
    }
  }
}
