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

class DataMonitorNodeInstance(override val nodeInfo: DataMonitorNode) extends ActionNodeInstance(nodeInfo)  {
  implicit val timeout = Timeout(60 seconds)
  
  override def execute(): Boolean = {
    //是否执行成功
    var result = true
    
    try {
      val monitorData = getData(nodeInfo.source)
      val maxDataOpt = if(nodeInfo.maxThre != null) Some(getData(nodeInfo.maxThre)) else None
      val minDataOpt = if(nodeInfo.minThre != null) Some(getData(nodeInfo.minThre)) else None
      
      //异常则发送邮件
      var defaultWarnMsg:String = null
      if(maxDataOpt.isDefined && monitorData > maxDataOpt.get){
        defaultWarnMsg = s"工作实例【${this.id}】中节点【${nodeInfo.name}】监控的数据值${monitorData}检测高于上限(${maxDataOpt.get})"
      }
      if(minDataOpt.isDefined && monitorData < minDataOpt.get){
        defaultWarnMsg = s"工作实例【${this.id}】中节点【${nodeInfo.name}】监控的数据值${monitorData}检测低于下限(${minDataOpt.get})"
      }
      var content:String = null
      if(defaultWarnMsg != null){
    	  content = if(nodeInfo.warnMsg == null || nodeInfo.warnMsg.trim() == "") defaultWarnMsg else nodeInfo.warnMsg
			  val titleMark = if(nodeInfo.isExceedError) "Error" else "Warn"
			  actionActor.sendMailMsg(null, s"【${titleMark}】data-monitor数据异常", content)
			  result = if(nodeInfo.isExceedError == true){
			    errorLog(content)
			    false 
			  }else {
			    warnLog(content)
			    result
			  }
      }
      
      //保存数据
      if(nodeInfo.isSaved){
        val dmr = DataMonitorRecord(nodeInfo.timeMark, nodeInfo.category, nodeInfo.sourceName, monitorData, minDataOpt,maxDataOpt, content, this.id)
        val persistManagerPath = this.actionActor.workflowActorRef.path / ".." / ".." / "pm"
        val persistManager = this.actionActor.context.actorSelection(persistManagerPath)
        persistManager ! Save(dmr)
      }
      result
    }catch{
      case e:Exception => 
        e.printStackTrace();
        errorLog(e.getMessage)
        false
    }
  }
  /**
   * 获取数据
   */
  def getData(obj: Any):Double = {
    val (stype, content, jdbcUrl,username,pwd) = obj match {
      case Source(typ,cont,info) => (typ,cont,info._1, info._2, info._3)
      case MaxThreshold(typ,cont,info) => (typ,cont,info._1, info._2, info._3)
      case MinThreshold(typ,cont,info) => (typ,cont,info._1, info._2, info._3)
      case _ => throw new Exception("未找到匹配的类型")
    }
    if(stype == MYSQL){
      getRmdbData("com.mysql.jdbc.Driver", content, jdbcUrl, username, pwd)
    }else if(stype == ORACLE){
      getRmdbData("oracle.jdbc.driver.OracleDriver", content, jdbcUrl, username, pwd)
    }else if(stype == HIVE){
      getRmdbData("org.apache.hive.jdbc.HiveDriver", content, jdbcUrl, username, pwd)
    }else if(stype == COMMAND) {  //COMMAND
      var filePath = Worker.config.getString("workflow.action.script-location") + "/" + s"action_${this.id}_${this.nodeInfo.name}"
      getCommandData(filePath, content)
    }else if(stype == NUM){
      getInputData(content)
    }else {
       throw new Exception("未找到适合的数据源类型")
    }
  }

    /**
     * 获取rmdb数据
     */
    private def getRmdbData(driverName: String, sql: String, jdbcUrl: String, username: String, pwd: String):Double = {
      var conn:Connection = null
      var stat:Statement = null
      try{
    	  Class.forName(driverName)
    	  //得到连接
    	  conn = DriverManager.getConnection(jdbcUrl, username, pwd)
    	  stat = conn.createStatement()
      	val rs = stat.executeQuery(sql)
      	val num = if(rs.next()){
        	  rs.getString(1).trim().toDouble
        	}else{
        	  throw new Exception("无查询结果")
        	}
       num
      }catch{
        case e:Exception => throw e
      }finally{
        if(stat != null) stat.close()
        if(conn != null) conn.close()
      }
    }
    
    /**
     * 获取命令数据
     */
    private def getCommandData(executeFilePath: String, content: String): Double = {
      //写入执行文件
      val lines = content.split("\n").filter { x => x.trim() != "" }.toList
      FileUtil.writeFile(executeFilePath,lines)
      FileUtil.setExecutable(executeFilePath, true)
      infoLog(s"写入到文件：${executeFilePath}")
      //执行
      infoLog(s"执行命令: ${executeFilePath}")
      val rsNum: String = s"${executeFilePath}" !!
      val num = rsNum.trim().toDouble
      num
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