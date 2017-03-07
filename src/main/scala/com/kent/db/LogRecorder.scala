package com.kent.db

import akka.actor.ActorLogging
import akka.actor.Actor
import java.sql.Connection
import java.sql.DriverManager
import com.kent.db.LogRecorder._
import com.kent.util.Util

class LogRecorder(url: String, username: String, pwd: String, isEnabled: Boolean) extends Actor with ActorLogging{
  implicit var connection: Connection = null
  def receive = print2Console
  if(isEnabled){
	  //注册Driver
	  Class.forName("com.mysql.jdbc.Driver")
	  //得到连接
	  connection = DriverManager.getConnection(url, username, pwd)
    context.become(print2DB)
  }
  /**
   * 开启打印到数据库
   */
  def print2DB: Actor.Receive = {
    case Info(ctype, sid, content) => logging("INFO", ctype, sid, Util.escapeStr(content))
    case Warn(ctype, sid, content) => logging("WARN", ctype, sid, Util.escapeStr(content))
    case Error(ctype, sid, content) => 
        logging("ERROR", ctype, sid, Util.escapeStr(content))
  }
  private def logging(level: String,ctype: String, sid: String, content: String):Boolean = {
    import com.kent.util.Util._
    val now = formatStandarTime(nowDate)
    val insertSql = s"""
      insert into log_record values(null,${withQuate(sid)},${withQuate(level)},${withQuate(ctype)},${withQuate(now)},${withQuate(content)})
      """
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
   * 打印到终端
   */
  def print2Console: Actor.Receive = {
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




