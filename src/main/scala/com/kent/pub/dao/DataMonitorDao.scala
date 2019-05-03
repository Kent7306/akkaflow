package com.kent.pub.dao

import java.sql.{Connection, SQLException}

import com.kent.daemon.DbConnector
import com.kent.workflow.node.action.DataMonitorRecord
import com.kent.util.Util._

/**
  * @author kent
  * @date 2019-04-26
  * @desc 数据监测节点记录数据操作类
  *
  **/
object DataMonitorDao {
  def save(record: DataMonitorRecord): Boolean = {
    val minStr = if(record.minOpt.isDefined) record.minOpt.get.toString() else null
    val maxStr = if(record.maxOpt.isDefined) record.maxOpt.get.toString() else null
    val insertSql = s"""
  	     insert into data_monitor values(${wq(record.sdate)},${wq(record.category)},${wq(record.sourceName)},
  	     ${wq(record.num.toString())},${wq(minStr)},${wq(maxStr)},${wq(record.wfiId)},${wq(record.remark)})
  	    """
    DbConnector.executeSyn(insertSql)
  }


  def update(record: DataMonitorRecord): Boolean = {
    val minStr = if(record.minOpt.isDefined) record.minOpt.get.toString() else null
    val maxStr = if(record.maxOpt.isDefined) record.maxOpt.get.toString() else null
    val updateSql = s"""
  	    update data_monitor set num = ${wq(record.num.toString())},
  	                            min = ${wq(minStr)},
  	                            max = ${wq(maxStr)},
  	                            workflow_instance_id = ${wq(record.wfiId)},
  	                            remark = ${wq(record.remark)}
  	    where sdate=${wq(record.sdate)}
          and category=${wq(record.category)}
          and name=${wq(record.sourceName)}
  	  """
    DbConnector.executeSyn(updateSql)
  }

  def isExist(record: DataMonitorRecord): Boolean = {
    val sql =
      s"""
         select * from data_monitor
         where sdate=${wq(record.sdate)} and category=${wq(record.category)}
           and name=${wq(record.sourceName)}"""
    DbConnector.querySyn[Boolean](sql, rs => {
      if (rs.next()) true else false
    }).get
  }

  def delete(record: DataMonitorRecord): Boolean = {
    val sql =
      s"""
         delete from data_monitor
         where sdate=${wq(record.sdate)} and category=${wq(record.category)}
           and name=${wq(record.sourceName)}
       """
    DbConnector.executeSyn(sql)
  }
  def merge(record: DataMonitorRecord): Boolean = {
    val isExists = this.isExist(record)
    if (isExists) update(record) else save(record)
  }
}
