package com.kent.daemon

import java.sql.{Connection, DriverManager, ResultSet, SQLException}
import java.util.Date

import akka.actor.{Actor, ActorRef}
import com.kent.daemon.LogRecorder.LogType.LogType
import com.kent.daemon.LogRecorder._
import com.kent.pub.Event._
import com.kent.pub._
import com.kent.pub.actor.Daemon
import com.kent.pub.dao.{LogRecordDao}
import com.kent.util.Util
import com.kent.util.Util._

import scala.util.Try

/**
  * 日志记录器
  */
class LogRecorder() extends Daemon{
  
  implicit var connection: Connection = _
  
  def individualReceive: Actor.Receive = {
    case Info(stime, ctype, sid, name, content) =>
      val record = LogMsg("INFO", ctype, sid, name, Util.escapeStr(content),formatStandardTime(stime))
      persist(stime, record)
    case Warn(stime, ctype, sid, name, content) =>
      val record = LogMsg("WARN", ctype, sid, name, Util.escapeStr(content),formatStandardTime(stime))
      persist(stime, record)
    case Error(stime, ctype, sid, name, content) =>
      val record = LogMsg("ERROR", ctype, sid, name, Util.escapeStr(content),formatStandardTime(stime))
      persist(stime, record)
    case GetLog(wfiId) => sender ! LogRecordDao.getLogWithWorkflowInstanceId(wfiId)
  }

  override def preStart(){
    log.info(s"log-recorder启动...")
  }
  
  override def postRestart(reason: Throwable){
    log.info(s"${reason.getMessage},log-recorder即将重启...")
    super.postRestart(reason)
  }


  var messageList:List[LogMsg] = List()
  var lastCommitTimeStamp = nowDate.getTime
  /**
   * 打印到数据库（这里用了个队列，一次性事务提交，减少数据库压力）
   */
  private def persist(stime: Date, record: LogMsg) = {
    //内容过长进行截取
    val newContent = if (record.content.getBytes.length >= 1024){
      val byteLen = record.content.getBytes("UTF-8")
      val bytes = byteLen.take(1010)
      val contentTmp = new String(bytes, "UTF-8")
      contentTmp.substring(0, contentTmp.length - 3)+"..."
    } else {
      record.content
    }
    record.content = newContent

    messageList = messageList :+ record

    if(messageList.nonEmpty || lastCommitTimeStamp - nowDate.getTime > 10000){
      messageList.foreach(LogRecordDao.save)
      messageList = List()
      lastCommitTimeStamp = nowDate.getTime
    }
  }
}


object LogRecorder {
  var actorOpt:Option[ActorRef] = None
  
  case class LogMsg(level: String,ctype: LogType, sid: String, name: String, var content: String,time: String)
  
  def apply():LogRecorder = new LogRecorder()
  
  
  object LogType extends Enumeration {
    type LogType = Value
    val WORFLOW_INSTANCE, ACTION_NODE_INSTANCE,WORKFLOW_MANAGER  = Value 
  }
  def info(ltyp: LogType, sid: String, name: String, content: String) = {
    if (actorOpt.isDefined){
      actorOpt.get ! Info(Util.nowDate, ltyp, sid, name, content)
    }
  }
  def error(ltyp: LogType, sid: String, name: String, content: String) = {
    if (actorOpt.isDefined) {
      actorOpt.get ! Error(Util.nowDate, ltyp, sid, name, content)
    }
  }
  def warn(ltyp: LogType, sid: String, name: String, content: String) = {
    if (actorOpt.isDefined) {
      actorOpt.get ! Warn(Util.nowDate, ltyp, sid, name, content)
    }
  }
  
}




