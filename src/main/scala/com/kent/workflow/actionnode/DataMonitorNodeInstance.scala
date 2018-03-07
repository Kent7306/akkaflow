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

      if(maxDataOpt.isDefined && monitorData > maxDataOpt.get){
      }else if(minDataOpt.isDefined && monitorData < minDataOpt.get){
      }else{
        return true
      }
      
      //异常则发送邮件
  	  val max = if(maxDataOpt.isDefined) maxDataOpt.get else "未定义"
  	  val min = if(minDataOpt.isDefined) minDataOpt.get else "未定义"
  	    
  	  if(nodeInfo.warnMsg == null || nodeInfo.warnMsg.trim() == ""){
  	    nodeInfo.warnMsg = "检测值未在范围内"
  	  }
  	  
  	  val content = s"""
        <style> 
        .table-n {text-align: center; border-collapse: collapse;border:1px solid black}
        h3 {margin-bottom: 5px}
        a {color:red;font-weight:bold}
        </style> 
        <h3>实例<data-monitor/>节点执行失败,内容如下</h3>
        
          <table class="table-n" border="1">
            <tr><td>实例ID</td><td>${this.id}</td></tr>
            <tr><td>节点名称</td><td>${nodeInfo.name}</td></tr>
            <tr><td>告警信息</td><td><a>${nodeInfo.warnMsg}</a></td></tr>
            <tr><td>上限</td><td><a>${max}</a></td></tr>
            <tr><td>检测值</td><td><a>${monitorData}</a></td></tr>
            <tr><td>下限</td><td><a>${min}</a></td></tr>
            <tr><td>总重试次数</td><td>${nodeInfo.retryTimes}</td></tr>
            <tr><td>当前重试次数</td><td>${this.hasRetryTimes}</td></tr>
            <tr><td>重试间隔</td><td>${nodeInfo.interval}秒</td></tr>
          </table>
          <a>&nbsp;<a>
        """
		  val titleMark = if(nodeInfo.isExceedError) "失败" else "警告"
		  actionActor.sendMailMsg(null, s"【Akkaflow】data-monitor节点执行${titleMark}", content)
		  result = if(nodeInfo.isExceedError == true){
		    errorLog(nodeInfo.warnMsg)
		    false 
		  }else {
		    warnLog(nodeInfo.warnMsg)
		    result
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
        val content = s"""
          <style> 
          .table-n {text-align: center; border-collapse: collapse;border:1px solid black}
          h3 {margin-bottom: 5px}
          </style> 
          <h3>实例<data-monitor/>节点执行失败,内容如下</h3>
          
            <table class="table-n" border="1">
              <tr><td>实例ID</td><td>${this.id}</td></tr>
              <tr><td>节点名称</td><td>${nodeInfo.name}</td></tr>
              <tr><td>告警信息</td><td><a style="color:red;font-weight:bold">${nodeInfo.warnMsg}</a></td></tr>
              <tr><td>出错信息</td><td>${e.getMessage}</td></tr>
              <tr><td>总重试次数</td><td>${nodeInfo.retryTimes}</td></tr>
              <tr><td>当前重试次数</td><td>${this.hasRetryTimes}</td></tr>
              <tr><td>重试间隔</td><td>${nodeInfo.interval}秒</td></tr>
            </table>
            <a>&nbsp;<a>
          """
        actionActor.sendMailMsg(null, s"【Akkaflow】data-monitor节点执行失败", content)
        errorLog(e.getMessage)
        this.executedMsg = s"(${e.getMessage})${nodeInfo.warnMsg}"
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
      var filePath = s"${this.executeDir}/run_data"
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
     try {
      val rsNum: String = s"${executeFilePath}" !!
      val num = rsNum.trim().toDouble
      num
     }catch{
       case e: Exception => throw e
     }finally{
    	 FileUtil.deleteDirOrFile(new File(executeFilePath))       
     }
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