package com.kent.db

import akka.actor.ActorLogging
import akka.actor.Actor
import java.sql.Connection
import java.sql.DriverManager
import com.kent.pub.Event._
import com.kent.util.Util
import com.kent.util.Util._
import com.kent.db.LogRecorder.LogMsg
import com.kent.pub.Daoable
import com.kent.pub.ActorTool

class LogRecorder(url: String, username: String, pwd: String, isEnabled: Boolean) extends ActorTool with Daoable[Any] {
  implicit var connection: Connection = null
  
  def indivivalReceive = print2Console
  if(isEnabled){
	  //注册Driver
	  Class.forName("com.mysql.jdbc.Driver")
	  //得到连接
	  connection = DriverManager.getConnection(url, username, pwd)
    context.become(print2DB orElse commonReceice)
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
  
  var messageList:List[LogMsg] = List()
  var lastCommitTimeStamp = nowDate.getTime
  /**
   * 打印到数据库（这里用了个队列，一次性事务提交，减少数据库压力）
   */
  private def logging(level: String,ctype: String, sid: String, content: String):Boolean = {
    val now = formatStandarTime(nowDate)
    messageList = messageList :+ LogMsg(level, ctype, sid, content, now)

    if(messageList.size > 100 || lastCommitTimeStamp - nowDate.getTime > 10000){
      val executeList = messageList
      messageList = List()
      val sqls = executeList.map { x => s"""
    		insert into log_record values(null,${withQuate(x.sid)},${withQuate(x.level)},${withQuate(x.ctype)},${withQuate(x.time)},${withQuate(x.content)})
      """ }.toList
      lastCommitTimeStamp = nowDate.getTime
    	executeSql(sqls)
    }else {
      true
    }
    
  }
  private def loggingStr(ctype: String, sid: String, content: String): String = {
    s"[${ctype}][${sid}] ${content}"
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

  def delete(implicit conn: Connection): Boolean = {
    ???
  }

  def getEntity(implicit conn: Connection): Option[Any] = {
    ???
  }

  def save(implicit conn: Connection): Boolean = {
    ???
  }
}
object LogRecorder {
  case class LogMsg(level: String,ctype: String, sid: String, content: String,time: String)
  
  def apply(url: String, username: String, pwd: String, isEnabled: Boolean):LogRecorder = new LogRecorder(url, username, pwd, isEnabled)
}




