package com.kent.pub.dao

import java.sql.Connection

import com.kent.daemon.DbConnector
import com.kent.lineage.LineageTable
import com.kent.util.Util.wq

/**
  * @author kent
  * @date 2019-04-22
  * @desc 血缘关系库表数据操作类
  *
  **/
object LineageTableDao {
  def get(name: String): Option[LineageTable] = {
    val isExistSql = s"""
      select * from lineage_table where name = '${name}'
      """
    DbConnector.querySyn[LineageTable](isExistSql, rs => {
      if(rs.next()){
        val t = LineageTable(name)
        t.lastUpdateTime = rs.getDate("last_update_time")
        t.createTime = rs.getDate("create_time")
        t.workflowName = rs.getString("workflow_name")
        t.owner = rs.getString("owner")
        t.dbLinkName = rs.getString("db_link_name")
        t.accessNum = rs.getInt("access_cnt")
        t
      }else{
        null
      }
    })
  }

  def merge(table: LineageTable): Boolean = {
    import com.kent.util.Util._
    val uptime = if(table.lastUpdateTime == null) null else formatStandarTime(table.lastUpdateTime)
    val ctime = if(table.createTime == null) null else formatStandarTime(table.createTime)
    this.delete(table.name)
    val insertSql = s"""
      insert into lineage_table values(
      ${wq(table.name)},${wq(uptime)},${wq(ctime)},
      ${wq(table.workflowName)},${wq(table.owner)},${wq(table.dbLinkName)},${table.accessNum})"""
    DbConnector.executeSyn(insertSql)
  }

  def update(table: LineageTable): Boolean = {
    import com.kent.util.Util._
    val uptime = if(table.lastUpdateTime == null) null else formatStandarTime(table.lastUpdateTime)
    val ctime = if(table.createTime == null) null else formatStandarTime(table.createTime)
    val updaeSql = s"""
      update lineage_table set last_update_time = ${wq(uptime)},
                               workflow_name = ${wq(table.workflowName)},
                               owner = ${wq(table.owner)},
                               db_link_name = ${wq(table.dbLinkName)}
      where name = ${wq(table.name)}
      """
    DbConnector.executeSyn(updaeSql)
  }

  def delete(name: String): Boolean = {
    val delSql = s"""
      delete from lineage_table where name = '${name}'
      """
    DbConnector.executeSyn(delSql)
  }

}
