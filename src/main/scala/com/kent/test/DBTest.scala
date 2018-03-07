package com.kent.test

import java.sql.DriverManager

object DBTest extends App{
   Class.forName("com.mysql.jdbc.Driver")
    //得到连接
    val connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/wf?useSSL=false", "root1", "root")
    val stat = connection.createStatement()
    stat.execute("delete from wf.log_recorder where 2 < 1")
}