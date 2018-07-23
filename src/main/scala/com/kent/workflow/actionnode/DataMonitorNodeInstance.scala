package com.kent.workflow.actionnode

import com.kent.workflow.node.ActionNodeInstance
import com.kent.workflow.node.NodeInstance
import scala.sys.process.ProcessLogger
import com.kent.main.Worker
import com.kent.pub.Event._
import com.kent.coordinate.ParamHandler
import scala.sys.process._
import java.util.Date
import java.io.PrintWriter
import java.io.File
import com.kent.util.Util
import akka.pattern.{ ask, pipe }
import scala.concurrent.ExecutionContext.Implicits.global
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.concurrent.Await
import com.kent.util.FileUtil
import com.kent.workflow.actionnode.DataMonitorNode.SourceType
import com.kent.workflow.actionnode.DataMonitorNode.SourceType._
import java.sql.Connection
import java.sql.Statement
import java.sql.DriverManager
import com.kent.workflow.actionnode.DataMonitorNode._
import java.sql.ResultSet
import com.kent.workflow.actionnode.DataMonitorNode.DatabaseType._
import com.kent.pub.db.MysqlOpera
import com.kent.pub.db.OracleOpera
import com.kent.pub.db.HiveOpera

class DataMonitorNodeInstance(override val nodeInfo: DataMonitorNode) extends ActionNodeInstance(nodeInfo)  {
  implicit val timeout = Timeout(60 seconds)
  DatabaseType
  
  override def execute(): Boolean = {
    this.executedMsg = "自定义消息："+nodeInfo.warnMsg
    var detectMsg = ""
    //是否执行成功
    val monitorData = getData(nodeInfo.source)
    val maxDataOpt = if(nodeInfo.maxThre != null) Some(getData(nodeInfo.maxThre)) else None
    val minDataOpt = if(nodeInfo.minThre != null) Some(getData(nodeInfo.minThre)) else None
    val result = 
      if((maxDataOpt.isDefined && monitorData > maxDataOpt.get) 
        || (minDataOpt.isDefined && monitorData < minDataOpt.get)){
      val max = if(maxDataOpt.isDefined) maxDataOpt.get else "未定义"
	    val min = if(minDataOpt.isDefined) minDataOpt.get else "未定义"
      detectMsg = s"检测值未在范围内，检测值:${monitorData}，下限:${min}，上限:${max}\n"
      detectMsg += "自定义信息："+ nodeInfo.warnMsg
  	  this.executedMsg = detectMsg
  	  false
    }else{
      true
    }
    
    //保存数据
    if(nodeInfo.isSaved){
      val dmr = DataMonitorRecord(nodeInfo.timeMark, nodeInfo.category, nodeInfo.sourceName, monitorData, minDataOpt,maxDataOpt, detectMsg, this.id)
      val persistManagerPath = this.actionActor.workflowActorRef.path / ".." / ".." / "pm"
      val persistManager = this.actionActor.context.actorSelection(persistManagerPath)
      persistManager ! Save(dmr)
    }
    result
  }
  /**
   * 获取数据
   */
  def getData(obj: Any):Double = {
    val (stype, content, dbLinkeNameOpt) = obj match {
      case Source(typ,cont,dbOpt) => (typ,cont, dbOpt)
      case MaxThreshold(typ,cont,dbOpt) => (typ,cont, dbOpt)
      case MinThreshold(typ,cont,dbOpt) => (typ,cont, dbOpt)
      case _ => throw new Exception("未找到匹配的类型")
    }
    if(stype == SQL){
      val dbLinkOptF = this.actionActor.getDBLink(dbLinkeNameOpt.get)
      val dbLinkOpt = Await.result(dbLinkOptF, 60 seconds)
      if(dbLinkOpt.isDefined) getRmdbData(content, dbLinkOpt.get) else throw new Exception
    }else if(stype == COMMAND) {  //COMMAND
      getCommandData(content)
    }else if(stype == NUM){
      getInputData(content)
    }else {
       throw new Exception("未找到适合的数据源类型")
    }
  }
  
    /**
     * 获取rmdb数据
     */
    private def getRmdbData(sql: String, dbLink: DBLink):Double = {
      def toNum(rs: ResultSet): Double = if(rs.next()) rs.getString(1).trim().toDouble else throw new Exception("无查询结果") 
      dbLink.dbType match {
        case MYSQL => MysqlOpera.querySql(sql, dbLink, toNum).get
        case ORACLE => OracleOpera.querySql(sql, dbLink, toNum).get
        case HIVE => HiveOpera.querySql(sql, dbLink, toNum).get
        case _ => throw new Exception(s"不存在db-link类型未${dbLink.dbType}")
      }
    }
    
    /**
     * 获取命令数据
     */
    private def getCommandData(content: String): Double = {
      //写入执行文件
      this.executeScript(content).trim().toDouble
    }
    /**
     * 获取直接输入的数据
     */
    private def getInputData(content: String):Double = {
      content.trim().toDouble
    }

  def kill(): Boolean = {
    true
  }
}