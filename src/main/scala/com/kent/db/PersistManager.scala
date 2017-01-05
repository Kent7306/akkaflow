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
import java.sql.ResultSet

class PersistManager(url: String, username: String, pwd: String, isEnabled: Boolean) extends Actor with ActorLogging{
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
    case Get(obj) =>  val resultObj = if(obj.getEntity.isEmpty) null else obj.getEntity.get; sender ! resultObj
    case Query(str) => sender ! queryList(str)
  }
  /**
   * 取消持久化
   */
  def passive: Actor.Receive = {
    case Get(obj) => sender ! null
    case _ => //do nothing!!!
  }
  /**
   * 查询结果数组
   */
  def queryList(sql: String): List[List[String]] = {
    val listOpt = querySql[List[List[String]]](sql, (rs: ResultSet) =>{
     var rowList = List[List[String]]()
     val colCnt = rs.getMetaData.getColumnCount
     while(rs.next()){
       val row = (1 to colCnt by 1).map { x => rs.getString(x) }.toList
       rowList = rowList :+ row
     }
     rowList
    })
    if(listOpt.isEmpty) null else listOpt.get
  }
  
  
  /**
   * 查询sql
   */
  private def querySql[A](sql: String, f:(ResultSet) => A)(implicit conn: Connection): Option[A] = {
    val stat = conn.createStatement()
    try{
    	val rs = stat.executeQuery(sql)
    	val obj = f(rs)
    	if(obj != null) return Some(obj)
    }catch{
      case e:Exception => e.printStackTrace()
    }finally{
      if(stat != null) stat.close()
    }
    None
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
  case class Query(query: String)
}