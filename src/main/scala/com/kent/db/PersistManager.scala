package com.kent.db

import akka.actor.ActorLogging
import akka.actor.Actor
import com.kent.pub.Daoable
import com.kent.workflow.node.NodeInfo
import java.sql.Connection
import java.sql.DriverManager
import com.kent.db.PersistManager._
import akka.actor.ActorRef
import com.kent.workflow.WorkflowInstance

class PersistManager(url: String, username: String, pwd: String, isEnabled: Boolean) extends Actor with ActorLogging {
  implicit var connection: Connection = null
  def receive = passive
  if(isEnabled){
	  //初始化数据连接 
	  //注册Driver
	  val driver = "com.mysql.jdbc.Driver"
	  Class.forName(driver)
	  //得到连接
	  connection = DriverManager.getConnection(url, username, pwd)
    context.become(active)
  }
  
  def active: Actor.Receive = {
    case Save(obj) => obj.save
    case Delete(obj) => obj.delete
    case Get(obj) => sender ! obj.getEntity.get
  }
  def passive: Actor.Receive = {
    case _ => //do nothing!!!
  }
  
  override def postStop(){
    if(connection != null)connection.close()
  }
}

object PersistManager {
  var pm: ActorRef = _
  def apply(url: String, username: String, pwd: String, isEnabled: Boolean):PersistManager = new PersistManager(url, username, pwd, isEnabled)
  case class Save[A](obj: Daoable[A])
  case class Delete[A](obj: Daoable[A])
  case class Get[A](obj: Daoable[A])
}