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
import com.kent.pub.DaemonActor
import com.kent.db.LogRecorder.LogType
import akka.actor.ActorRef
import java.util.Date

class LogRecorder(url: String, username: String, pwd: String, isEnabled: Boolean) extends DaemonActor with Daoable[Any] {
  import com.kent.db.LogRecorder.LogType._
  
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
    case Info(stime, ctype, sid, name, content) => logging("INFO", stime, ctype, sid, name, Util.escapeStr(content))
    case Warn(stime, ctype, sid, name, content) => logging("WARN", stime, ctype, sid, name, Util.escapeStr(content))
    case Error(stime, ctype, sid, name, content) => logging("ERROR", stime, ctype, sid, name, Util.escapeStr(content))
  }
  
  var messageList:List[LogMsg] = List()
  var lastCommitTimeStamp = nowDate.getTime
  /**
   * 打印到数据库（这里用了个队列，一次性事务提交，减少数据库压力）
   */
  private def logging(level: String,stime: Date,ctype: LogType, sid: String, name: String, content: String):Boolean = {
    val st = formatStandarTime(stime)
    messageList = messageList :+ LogMsg(level, ctype.toString(), sid,name, content, st)

    if(messageList.size > 0 || lastCommitTimeStamp - nowDate.getTime > 10000){
      val executeList = messageList
      messageList = List()
      val sqls = executeList.map { x => s"""
    		insert into log_record values(null,${withQuate(x.level)},${withQuate(x.time)},${withQuate(x.ctype)},${withQuate(x.sid)},${withQuate(x.name)},${withQuate(x.content)})
      """ }.toList
      lastCommitTimeStamp = nowDate.getTime
    	executeSql(sqls)
    }else {
      true
    }
    
  }
  
  private def loggingStr(stime: Date, ctype: LogType, sid: String, name: String, content: String): String = {
    s"[${ctype}][${sid}][${name}] ${content}"
  }
  /**
   * 打印到终端
   */
  def print2Console: Actor.Receive = {
    case Info(stime, ctype, sid, name, content) => log.info(loggingStr(stime,ctype, sid, name, content))
    case Warn(stime, ctype, sid, name, content) => log.warning(loggingStr(stime,ctype, sid, name, content))
    case Error(stime, ctype, sid, name, content) => log.error(loggingStr(stime,ctype, sid, name, content))
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
  var actor:ActorRef = _
  
  case class LogMsg(level: String,ctype: String, sid: String, name: String, content: String,time: String)
  
  def apply(url: String, username: String, pwd: String, isEnabled: Boolean):LogRecorder = new LogRecorder(url, username, pwd, isEnabled)
  
  
  object LogType extends Enumeration {
    type LogType = Value
    val COORDINATOR, WORFLOW_INSTANCE, ACTION_NODE_INSTANCE,WORKFLOW_MANAGER  = Value 
  }
  
  import com.kent.db.LogRecorder.LogType._
  def info(ltyp: LogType, sid: String, name: String, content: String) = actor ! Info(Util.nowDate, ltyp, sid, name, content)
  def error(ltyp: LogType, sid: String, name: String, content: String) = actor ! Error(Util.nowDate, ltyp, sid, name, content)
  def warn(ltyp: LogType, sid: String, name: String, content: String) = actor ! Warn(Util.nowDate, ltyp, sid, name, content)
  
}




