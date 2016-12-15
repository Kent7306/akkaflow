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
	  //注册Driver
	  Class.forName("com.mysql.jdbc.Driver")
	  //得到连接
	  connection = DriverManager.getConnection(url, username, pwd)
    context.become(active)
  }
  /**
   * 开启持久化
   */
  def active: Actor.Receive = {
    case Save(obj) => obj.save
    case Delete(obj) => obj.delete
    case Get(obj) => sender ! obj.getEntity.get
  }
  /**
   * 取消持久化
   */
  def passive: Actor.Receive = {
    case _ => //do nothing!!!
  }
  
  override def postStop(){
    if(connection != null)connection.close()
  }
}

object PersistManager {
  def apply(url: String, username: String, pwd: String, isEnabled: Boolean):PersistManager = new PersistManager(url, username, pwd, isEnabled)
  case class Save[A](obj: Daoable[A])
  case class Delete[A](obj: Daoable[A])
  case class Get[A](obj: Daoable[A])
}