package com.kent.workflow.node.action.transfer.target

import java.sql.Connection
import com.kent.pub.Daoable
import com.kent.workflow.node.action.transfer.source.Source.Column
import com.kent.pub.db._
import com.kent.pub.db.DBLink.DatabaseType._

  abstract class GennericDBTarget(isPreTruncate: Boolean, dbLink: DBLink, 
                 table: String, preOpera: String,afterOpera: String) extends Target with Daoable {
    var conn: Connection = _
    var colNum: Int = 0
    
    def init(): Boolean = {
    conn = dbLink.dbType match {
        case MYSQL => MysqlOpera.getConnection(dbLink)
        case ORACLE => OracleOpera.getConnection(dbLink)
        case HIVE => HiveOpera.getConnection(dbLink)
        case _ => throw new Exception(s"不存在db-link类型未${dbLink.dbType}")
      }
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
      val preSqls = if(preOpera != null) preOpera.split(";").map(_.trim()).filter { _ != "" }.toList else List()
      var sqls = clearSql ++ preSqls
      executeSqls(sqls)(conn)
    }

    def afterOpera(): Boolean = {
      val afterSqls = if(afterOpera != null) afterOpera.split(";").map(_.trim()).filter { _ != "" }.toList else List()
      executeSqls(afterSqls)(conn)
    }

    def getColNum: Option[Int] = {
      if(this.colNum == 0){
      val num = querySql(s"select * from ${table} where 1 < 0", rs => {
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