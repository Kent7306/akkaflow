package com.kent.daemon

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Date

import akka.actor.{Actor, ActorRef}
import com.kent.daemon.LogRecorder.LogType.LogType
import com.kent.daemon.LogRecorder._
import com.kent.pub.Event._
import com.kent.pub._
import com.kent.pub.actor.Daemon
import com.kent.util.Util
import com.kent.util.Util._

/**
  * 日志记录器
  * @param url
  * @param username
  * @param pwd
  * @param isEnabled
  */
class LogRecorder(url: String, username: String, pwd: String, isEnabled: Boolean) extends Daemon with Daoable {
  
  implicit var connection: Connection = null
  
  def individualReceive = print2Console

  override def preStart(){
    init()
  }
  
  override def postRestart(reason: Throwable){
    log.info(s"${reason.getMessage},log-recorder即将重启...")
    super.postRestart(reason)
  }

  def init(): Unit ={
    if(isEnabled){
      //注册Driver
      Class.forName("com.mysql.jdbc.Driver")
      //得到连接
      connection = DriverManager.getConnection(url, username, pwd)
      context.become(print2DB orElse commonReceice)
    }
  }
  
  /**
   * 开启打印到数据库
   */
  def print2DB: Actor.Receive = {
    case Info(stime, ctype, sid, name, content) => logging("INFO", stime, ctype, sid, name, Util.escapeStr(content))
    case Warn(stime, ctype, sid, name, content) => logging("WARN", stime, ctype, sid, name, Util.escapeStr(content))
    case Error(stime, ctype, sid, name, content) => logging("ERROR", stime, ctype, sid, name, Util.escapeStr(content))
    case GetLog(ctype,sid,name) => sender ! getLog(ctype, sid, name)
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
    	executeSqls(sqls)
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
    case GetLog(ctype,sid,name) => sender ! List[List[String]]()
  }
  
  override def postStop(){
    if(connection != null)connection.close()
  }
  /**
   * 获取日志
   */
  def getLog(ctype: LogType, sid: String,name: String):List[List[String]] = {
    val c1 = if(ctype != null) s"ctype = '${ctype}'" else "1 = 1";
    val c2 = if(sid != null) s"sid = '${sid}'" else "1 = 1";
    val c3 = if(name != null) s"name = '${name}'" else "1 = 1";
    val sql = s"""
      select level,stime,name,content from log_record where ${c1} and ${c2} and ${c3} order by stime
      """
    queryList(sql)
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
}


object LogRecorder {
  var actor:ActorRef = _
  
  case class LogMsg(level: String,ctype: String, sid: String, name: String, content: String,time: String)
  
  def apply(url: String, username: String, pwd: String, isEnabled: Boolean):LogRecorder = new LogRecorder(url, username, pwd, isEnabled)
  
  
  object LogType extends Enumeration {
    type LogType = Value
    val WORFLOW_INSTANCE, ACTION_NODE_INSTANCE,WORKFLOW_MANAGER  = Value 
  }
  def info(ltyp: LogType, sid: String, name: String, content: String) = actor ! Info(Util.nowDate, ltyp, sid, name, content)
  def error(ltyp: LogType, sid: String, name: String, content: String) = actor ! Error(Util.nowDate, ltyp, sid, name, content)
  def warn(ltyp: LogType, sid: String, name: String, content: String) = actor ! Warn(Util.nowDate, ltyp, sid, name, content)
  
}




