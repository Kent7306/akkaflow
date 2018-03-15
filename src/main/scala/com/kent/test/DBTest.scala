package com.kent.test

import java.sql.DriverManager

object DBTest extends App{
   Class.forName("com.mysql.jdbc.Driver")
    //得到连接
    val connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/wf?useSSL=false", "root", "root")
    val stat = connection.createStatement()
    val result = stat.execute("delete from wf.log_record where sid = '1b472328'")
    println(result)
}