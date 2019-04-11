package com.kent.lineage

import java.sql.Connection
import java.util.Date

import com.kent.pub.{DeepCloneable, Persistable}

/**
 * 血缘关系模型
 * 持久化到数据库中
 */
class LineageTable extends DeepCloneable[LineageTable] with Persistable[LineageTable] {
  var name: String = _
  var lastUpdateTime: Date = _
  var createTime: Date = _
  var workflowName: String = _
  var owner: String = _
  var accessNum: Long = 0
  var dbLinkName: String = _
  var parents: List[String] = _

  def save(implicit conn: Connection): Boolean = {
    import com.kent.util.Util._
    val uptime = if(lastUpdateTime == null) null else formatStandarTime(lastUpdateTime)
    val ctime = if(createTime == null) null else formatStandarTime(createTime)
    
    val insertSql = s"""
      insert into lineage_table values(
      ${wq(name)},${wq(uptime)},${wq(ctime)},
      ${wq(workflowName)},${wq(owner)},${wq(dbLinkName)},${accessNum})"""
    
    val updaeSql = s"""
      update lineage_table set last_update_time = ${wq(uptime)},
                               workflow_name = ${wq(workflowName)},
                               owner = ${wq(owner)},
                               db_link_name = ${wq(dbLinkName)}
      where name = ${wq(name)}             
      """
    val existEntityOpt = this.getEntity
    //merge
    if(existEntityOpt.isEmpty){
      executeSql(insertSql)
    }else{
      executeSql(updaeSql)
    }
    true
  }
  
  def delete(implicit conn: Connection): Boolean = {
    val delSql = s"""
      delete from lineage_table where name = '${name}'
      """
    executeSql(delSql)
  }

  def getEntity(implicit conn: Connection): Option[LineageTable] = {
    val isExistSql = s"""
      select * from lineage_table where name = '${name}'
      """
    val entityOpt = querySql(isExistSql, rs => {
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
    entityOpt
  }  
}

object LineageTable {
  def apply(name: String): LineageTable = {
    val t = new LineageTable()
    t.name = name
    t
  }
}