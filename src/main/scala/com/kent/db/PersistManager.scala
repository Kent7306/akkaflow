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
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import com.kent.pub.ActorTool
import com.kent.pub.DaemonActor
import java.net.ConnectException
import com.mysql.jdbc.exceptions.jdbc4.CommunicationsException
import java.sql.Statement
import com.kent.pub.CustomException._
import java.sql.SQLException

class PersistManager(url: String, username: String, pwd: String, isEnabled: Boolean) extends DaemonActor{
  implicit var connection: Connection = null
  //只需要第一次启动时初始化建表sql
  var isInitSqlNeeded = true 
  
  def indivivalReceive = passive

  override def preStart(){
    init()
  }
  override def postRestart(reason: Throwable){
    this.isInitSqlNeeded = false
    log.info(s"${reason.getMessage},pm管理器即将重启...")
    super.postRestart(reason)
  }
  
  /**
   * 初始化
   */
  def init() = {
    if(isEnabled){
      context.become(active orElse commonReceice)
      try {
        //注册Driver
  	    Class.forName("com.mysql.jdbc.Driver")
  	    //得到连接
  	    connection = DriverManager.getConnection(url, username, pwd)
      } catch {
        case e: Exception => 
          log.error("连接数据库失败，请检查数据库配置")
          //context.become(passive orElse commonReceice)
      } 
    }else{
      log.warning("数据库功能未开启...")
    }
    if(isInitSqlNeeded) initSql()
  }
  
  @throws(classOf[SQLException])
  def initSql() = {
    var stat:Statement = null
    if(connection != null){
      try {
  	    //启动清理
        var content = ""
        Source.fromFile(this.getClass.getResource("/").getPath + "../config/create_table.sql").foreach { content += _ }
        val sqls = content.split(";").filter { _.trim() !="" }.toList
        connection.setAutoCommit(false)
      	stat = connection.createStatement()
        val results = sqls.map { stat.execute(_) }.toList
        connection.commit()
      } catch {
        case e: Exception =>
          e.printStackTrace()
          connection.rollback()
          log.error("执行初始化建表sql失败")
          throw e
      } finally{
        if(stat != null) stat.close()
        connection.setAutoCommit(true)
      }
      log.info("成功初始化数据库")
    }
  }
  /**
   * 开启持久化
   */
  def active: Actor.Receive = {
    //??? 后续改成future
    case Save(obj) => obj.save
    case Delete(obj) => sender ! obj.delete
    case Get(obj) => sender ! obj.getEntity
    //case Get(obj) => f(sender, obj)
    case ExecuteSql(sql) => sender ! executeSql(sql)
    case Query(str) => sender ! queryList(str)
  }
  
  /**
   * 取消持久化
   */
  def passive: Actor.Receive = {
    case Get(obj) => sender ! None
    case Save(obj) => 
    case ExecuteSql(sql) => sender ! true
    case Delete(obj) => sender ! true
    case Query(str) => sender ! List[List[String]]()
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
  /**
   * 执行sql
   */
  def executeSql(sql: String)(implicit conn: Connection): Boolean = {
    val stat = conn.createStatement()
    var result:Boolean = true
    try{
      stat.execute(sql) 
    }catch{
      case e:Exception => e.printStackTrace()
      result = false
    }finally{
      if(stat != null) stat.close()
      result
    }
    result
  }
  
  override def postStop(){
    if(connection != null)connection.close()
  }
}

object PersistManager {
  def apply(url: String, username: String, pwd: String, isEnabled: Boolean):PersistManager = new PersistManager(url, username, pwd, isEnabled)
}