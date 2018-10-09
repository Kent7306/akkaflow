package com.kent.test

import java.sql.DriverManager
import com.kent.workflow.actionnode.transfer.source.Source._
import java.sql.ResultSetMetaData
import com.kent.pub.db.OracleOpera
import com.kent.pub.Event.DBLink
import com.kent.workflow.actionnode.DataMonitorNode.DatabaseType

object DBTest extends App{
  val dblink = DBLink(DatabaseType.ORACLE,"111","jdbc:oracle:thin:@//localhost:49161/XE","dw","dw")
   OracleOpera.getConnection(dblink)
}