package com.kent.workflow.node.action.transfer.target.io

import java.sql.Connection

import com.kent.pub.db.{Column, DBLink, MysqlOperator}
import com.kent.workflow.node.action.transfer.target.Target

class MysqlTarget(actionName: String, instanceId: String, isPreTruncate: Boolean, dbLink: DBLink,
                 table: String, preOperaStr: String,afterOperaStr: String) extends Target(actionName, instanceId) {
  var conn: Connection = _

  def init(): Boolean = {
    //这里能够高性能批量处理
    if (dbLink.jdbcUrl.contains("?") && dbLink.jdbcUrl.contains("="))
      dbLink.jdbcUrl = dbLink.jdbcUrl + "&useServerPrepStmts=false&rewriteBatchedStatements=true"
    conn = MysqlOperator.getConnection(dbLink)
    conn.setAutoCommit(false)
    true
  }

  def persist(rows: List[List[String]]): Boolean = {
    totalRowNum += rows.size
    val colLen = this.columns.get.size
    val paramLine = (1 to colLen).map{x => "?"}.mkString(",")
    val insertSql = s"insert into $table values($paramLine)"
    if(rows.exists(_.size != colLen)) throw new Exception(s"存在某（多条）记录的字段个数不等于$colLen")
    MysqlOperator.executeBatch(insertSql, rows)(conn)
    true
  }

  def preOpera(): Boolean = {
    val clearSql = if(isPreTruncate) List(s"delete from ${table} where 1 = 1") else List()
    val preSqls = if(preOperaStr != null) preOperaStr.split(";").map(_.trim()).filter { _ != "" }.toList else List()
    var sqls = clearSql ++ preSqls
    if (sqls.size > 0) {
      infoLog("执行前置语句")
      MysqlOperator.executeSqls(sqls, dbLink)
    }
    true
  }

  def afterOpera(): Boolean = {
    val afterSqls = if(afterOperaStr != null) afterOperaStr.split(";").map(_.trim()).filter { _ != "" }.toList else List()
    if (afterSqls.size > 0) {
      infoLog("执行后置语句")
      MysqlOperator.executeSqls(afterSqls, dbLink)
    }
    true
  }

  def finish(isSuccessed: Boolean): Unit = {
    if(!isSuccessed && conn != null){
      conn.rollback()
    }else if(conn != null) {
      conn.commit()
    }
    if(conn != null) conn.close()
  }
  /**
    * 获取目标字段个数
    */
  override def getColsWithSourceCols(sourceCols: List[Column]): Option[List[Column]] = {
    //目标表不存在则创建
    val isTableExists = MysqlOperator.isTableExist(table, dbLink)
    if (isTableExists){
      infoLog("目标表存在")
    } else {
      infoLog("目标表不存在，将自动建表")
      MysqlOperator.createTable(table, sourceCols, dbLink, createSql => {
        infoLog(createSql)
      })
    }
    val cols = MysqlOperator.getColumns(s"select * from ${table} where 1 < 0", dbLink)
    Some(cols)
  }
}

object MysqlTarget {

}