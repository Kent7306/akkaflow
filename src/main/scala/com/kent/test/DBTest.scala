package com.kent.test

import java.sql.DriverManager
import com.kent.workflow.node.action.transfer.source.Source._
import java.sql.ResultSetMetaData
import com.kent.pub.db.OracleOpera
import com.kent.pub.db.DBLink
import com.kent.pub.db.DBLink.DatabaseType
import com.kent.workflow.node.action.MetadataNode.SelfColumn

object DBTest extends App{
  val dblink = DBLink(DatabaseType.ORACLE,"111","jdbc:oracle:thin:@//192.168.31.246:1521/XE","etl","root")
  var sql = s"select column_name,comments from all_col_comments where table_name='${"USER_INFO".toUpperCase()}'"
  sql = sql + s" and owner = '${"DW".toUpperCase()}'"
  println(sql)
  val columns = OracleOpera.querySql(sql, dblink, rs => {
    while(rs.next()){
      val cmmt = if(rs.getString(2) == null) "1" else "0"
      println(SelfColumn(rs.getString(1).toLowerCase(), null, cmmt))
    }
    111
  })
   
}