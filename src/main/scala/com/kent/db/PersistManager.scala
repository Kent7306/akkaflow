package com.kent.db

import akka.actor.ActorLogging
import akka.actor.Actor
import com.kent.pub.Daoable
import com.kent.workflow.node.NodeInfo
import java.sql.Connection
import java.sql.DriverManager
import com.kent.pub.Event._
import akka.actor.ActorRef
import com.kent.workflow.WorkflowInstance
import java.sql.ResultSet
import scala.io.Source

class PersistManager(url: String, username: String, pwd: String, isEnabled: Boolean) extends Actor with ActorLogging{
  implicit var connection: Connection = null
  def receive = passive
  if(isEnabled){
	  //注册Driver
	  Class.forName("com.mysql.jdbc.Driver")
	  //得到连接
	  connection = DriverManager.getConnection(url, username, pwd)
    context.become(active)
    init()
  }
  /**
   * init
   */
  def init(){
    var content = ""
    log.info("检查库表情况...")
    Source.fromFile(this.getClass.getResource("/").getPath + "../config/create_table.sql").foreach { content += _ }
    val sqls = content.split(";").filter { _.trim() !="" }.toList
    try {
      connection.setAutoCommit(false)
    	val stat = connection.createStatement()
      val results = sqls.map { stat.execute(_) }.toList
      connection.commit()
    } catch {
      case e: Exception => 
          connection.rollback()
          e.printStackTrace()
          throw new Exception("执行初始化建表sql失败")
    }
    
    connection.setAutoCommit(true)
    log.info("初始化数据库成功...")
  }
  /**
   * 开启持久化
   */
  def active: Actor.Receive = {
    case Save(obj) => obj.save
    case Delete(obj) => obj.delete
    case Get(obj) =>  val resultObj = if(obj.getEntity.isEmpty) null else obj.getEntity.get; sender ! obj.getEntity
    case Query(str) => sender ! queryList(str)
  }
  /**
   * 取消持久化
   */
  def passive: Actor.Receive = {
    case Get(obj) => sender ! None
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
}