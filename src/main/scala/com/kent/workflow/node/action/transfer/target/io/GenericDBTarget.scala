package com.kent.workflow.node.action.transfer.target.io

import java.sql.Connection

import com.kent.pub.db._
import com.kent.workflow.node.action.transfer.target.Target

/**
  * 通用数据库目标类
  * @param isPreTruncate
  * @param dbLink
  * @param table
  * @param preOperaStr
  * @param afterOperaStr
  */
abstract class GenericDBTarget(actionName: String, instanceId: String, isPreTruncate: Boolean, dbLink: DBLink,
                               table: String, preOperaStr: String, afterOperaStr: String) extends Target(actionName, instanceId) {
    implicit var conn: Connection = _
    var colNum: Int = 0
    var jdbcOpera: JdbcOperator = _

    def init(): Boolean = {
      jdbcOpera = dbLink.getJdbcOpera()
      conn = jdbcOpera.getConnection(dbLink)
      true
    }

    def persist(rows: List[List[String]]): Boolean = {
      val paramLine = (1 to getColNum.get).map{x => "?"}.mkString(",")
      val insertSql = s"insert into ${table} values(${paramLine})"
      totalRowNum += rows.size
      if(rows.filter { _.size != getColNum.get }.size > 0) throw new Exception(s"存在某（多条）记录的字段个数不等于${getColNum.get}")
      //executeBatch(insertSql, rows)(conn)
      ???
    }

    def preOpera(): Boolean = {
      val clearSql = if(isPreTruncate) List(s"delete from ${table} where 1 = 1") else List()
      val preSqls = if(preOperaStr != null) preOperaStr.split(";").map(_.trim()).filter { _ != "" }.toList else List()
      var sqls = clearSql ++ preSqls
      jdbcOpera.execute(sqls)
    }

    def afterOpera(): Boolean = {
      val afterSqls = if(afterOperaStr != null) afterOperaStr.split(";").map(_.trim()).filter { _ != "" }.toList else List()
      jdbcOpera.execute(afterSqls)
    }

    def getColNum: Option[Int] = {
      if(this.colNum == 0){
      val num = jdbcOpera.query(s"select * from ${table} where 1 < 0", rs => {
    	  rs.getMetaData.getColumnCount
      })(conn).get
      this.colNum = num
      }
      Some(this.colNum)
    }

    def finish(isSuccessed: Boolean) = {
      if(!isSuccessed && conn != null) conn.rollback() else if(conn != null) conn.commit()
      if(conn != null) conn.close()
      
    }
    
    def createTableIfNotExist(cols: List[Column])
  }