package com.kent.test

import com.kent.workflow.node.Node
import java.sql.DriverManager
import com.kent.workflow.WorkflowInstance

object ReflectTest extends App{
  Class.forName("com.mysql.jdbc.Driver")
  //得到连接
  implicit val connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/wf?useSSL=false", "root", "root")

  val wfi = WorkflowInstance("79ff74bd")
  
}