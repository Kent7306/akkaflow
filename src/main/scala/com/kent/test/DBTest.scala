package com.kent.test

import java.sql.DriverManager
import com.kent.workflow.actionnode.transfer.source.Source._
import java.sql.ResultSetMetaData

object DBTest extends App{
   Class.forName("com.mysql.jdbc.Driver")
    //得到连接
    val connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/wf?useSSL=false", "root", "root")
    val stat = connection.createStatement()
    val rs = stat.executeQuery("select * from wf.ccc where 2 > 3")
    (1 to rs.getMetaData.getColumnCount).map{idx =>
      println(rs.getMetaData.getColumnName(idx) + "  " + rs.getMetaData.getColumnTypeName(idx) 
              + "  " + rs.getMetaData.getColumnType(idx) + "  " + rs.getMetaData.getColumnClassName(idx) + "  "
              + rs.getMetaData.getPrecision(idx))
   }
   println("-------------------\n")
   println(getColnums(rs.getMetaData))
   
   
   def getColnums(md: ResultSetMetaData): List[Column] = {
     (1 to md.getColumnCount).map{ idx =>
       md.getColumnTypeName(idx) match {
         case x if(x == "VARCHAR" || x == "CHAR") => Column(md.getColumnName(idx), DataType.STRING,md.getColumnType(idx), 0)
         case x if(x == "DATE" || x == "TIME" || x == "DATETIME" || x == "TIMESTAMP" || x == "YEAR") => Column(md.getColumnName(idx), DataType.STRING, md.getPrecision(idx), 0)
         case x if(x.contains("INT")) => Column(md.getColumnName(idx), DataType.NUMBER, md.getPrecision(idx), 0)
         case x if(x == "DOUBLE") =>  Column(md.getColumnName(idx), DataType.NUMBER, 16, 8)
         case x if(x == "DECIMAL") => Column(md.getColumnName(idx), DataType.NUMBER, 16, 8)
         case other => throw new Exception(s"未配置映射的mysql数据类型: ${other}")
       }
     }.toList
   }
   
}