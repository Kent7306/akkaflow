package com.kent.db

import akka.actor.ActorLogging
import akka.actor.Actor
import java.sql.Connection
import java.sql.DriverManager
import com.kent.db.LogRecorder._

class LogRecorder(url: String, username: String, pwd: String, isEnabled: Boolean) extends Actor with ActorLogging{
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
   * 开启保存到数据库
   */
  def active: Actor.Receive = {
    case Info(ctype, sid, content) => logging("INFO", ctype, sid, content)
    case Warn(ctype, sid, content) => logging("WARN", ctype, sid, content)
    case Error(ctype, sid, content) => logging("ERROR", ctype, sid, content)
  }
  private def logging(level: String,ctype: String, sid: String, content: String):Boolean = {
    import com.kent.util.Util._
    val now = formatStandarTime(nowDate)
    val insertSql = s"""
      insert into log_record values(null,${withQuate(sid)},${withQuate(level)},${withQuate(ctype)},${withQuate(now)},${withQuate(content)})
      """
      
      println(insertSql)
    executeSql(insertSql)
  }
  private def loggingStr(ctype: String, sid: String, content: String): String = {
    s"[${ctype}][${sid}] ${content}"
  }
  
  def executeSql(sql: String)(implicit conn: Connection): Boolean = {
    val stat = conn.createStatement()
    var result:Boolean = false
    try{
    	result = stat.execute(sql)      
    }catch{
      case e:Exception => e.printStackTrace()
    }finally{
      if(stat != null) stat.close()
      result
    }
    result
  }
  /**
   * 消极处理方法
   */
  def passive: Actor.Receive = {
    case Info(ctype, sid, content) => log.info(loggingStr(ctype, sid, content))
    case Warn(ctype, sid, content) => log.warning(loggingStr(ctype, sid, content))
    case Error(ctype, sid, content) => log.error(loggingStr(ctype, sid, content))
  }
  
  override def postStop(){
    if(connection != null)connection.close()
  }
}
object LogRecorder {
  def apply(url: String, username: String, pwd: String, isEnabled: Boolean):LogRecorder = new LogRecorder(url, username, pwd, isEnabled)
  case class Info(ctype: String, sid: String, content: String)
  case class Warn(ctype: String, sid: String, content: String)
  case class Error(ctype: String, sid: String, content: String)
}




