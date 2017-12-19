package com.kent.workflow.actionnode.transfer

import akka.actor.Actor
import akka.actor.ActorLogging
import com.kent.workflow.actionnode.TransferNode.ConnectType._
import com.kent.workflow.actionnode.TransferNode._
import java.sql.ResultSet
import java.sql.Statement
import java.sql.Connection
import java.sql.DriverManager
import scala.util.Try
import scala.util.Failure
import scala.util.Success
import java.io.File
import java.sql.PreparedStatement
import com.kent.db.LogRecorder
import com.kent.db.LogRecorder.LogType._
import com.kent.db.LogRecorder.LogType
import scala.collection.immutable.List
import scala.io.BufferedSource

class Source(sourceInfo: SourceInfo) extends  Actor with ActorLogging 
{
  private var isReadEnd = false
  
  private var conn: Connection = null
  private var stat: PreparedStatement = null
  private var rs: ResultSet = null
  
  private var bs: BufferedSource = null
  private var iter: Iterator[String] = null
  
  private var rowNum: Int = 0
  private var colNum: Int = 0
  private val ROW_MAX_SIZE: Int = 5000
  
  override def postStop(): Unit = {
    if(bs != null) bs.close()
    if(stat != null) stat.close()
    if(conn != null) conn.close()
  }
  
  def receive: Actor.Receive = {
    case GetColNum() => sender ! getColNum()
    case GetRows() => 
      sender ! fillRowBuffer()
      if(isReadEnd) context.stop(self)
  }
  
  def fillRowBuffer(): Try[Rows] = {
    Try{
      if(sourceInfo.sType == LFS){
        val soureFileInfo = sourceInfo.asInstanceOf[SourceFileInfo]
        if(iter == null){
          bs = scala.io.Source.fromFile(soureFileInfo.path)
          iter = bs.getLines()
        }
        val rowsBuffer = scala.collection.mutable.ArrayBuffer[List[String]]()
        var cnt = 0
        while (iter.hasNext && cnt < ROW_MAX_SIZE) {
          rowsBuffer.append(iter.next().split(soureFileInfo.delimited).toList)
        }
        //结尾
        if(cnt != ROW_MAX_SIZE){
          isReadEnd = true
          rowsBuffer.append(null)
        }
        Rows(rowsBuffer.toList)
      }else{
        val sourceJdbcInfo = sourceInfo.asInstanceOf[SourceJdbcInfo]
        if(rs == null) {
          val parseTable = if(sourceJdbcInfo.tableSql.contains(" ")) s"(${sourceJdbcInfo.tableSql})" else sourceJdbcInfo.tableSql
          stat = conn.prepareStatement(s"select * from ${parseTable} AAA_BBB_CCC")
          rs = stat.executeQuery()
        }
        val rowsBuffer = scala.collection.mutable.ArrayBuffer[List[String]]()
        var cnt = 0
        while (rs.next() && cnt < ROW_MAX_SIZE) {
          cnt += 1
          val row = (1 to colNum).map(rs.getString(_)).map { _.replaceAll("(\n|\r)+", " ") }.toList
          rowsBuffer.append(row)
        }
        //结尾
        if(cnt != ROW_MAX_SIZE){
          isReadEnd = true
          rowsBuffer.append(null)
        }
        Rows(rowsBuffer.toList)
      }
    }
  }
  
  /**
   * 返回数据源的（字段数，记录总数）
   */
  def getColNum(): Try[ColNum] = {
    if(sourceInfo.sType == LFS) {
      val soureFileInfo = sourceInfo.asInstanceOf[SourceFileInfo]
    	if(!new File(soureFileInfo.path).exists()){
    	  return Failure(new Exception("数据源文件不存在"))
    	}
      val sTmp = scala.io.Source.fromFile(soureFileInfo.path)
      colNum = if(sTmp.hasNext) sTmp.getLines().next().split(soureFileInfo.delimited).size else 0
      sTmp.close()
      Success(ColNum(colNum))
    }else{  //JDBC
    	val sourceJdbcInfo = sourceInfo.asInstanceOf[SourceJdbcInfo]
      val connTry = Source.getJdbcConnection(sourceJdbcInfo.sType, sourceJdbcInfo.jdbcUrl, sourceJdbcInfo.username, sourceJdbcInfo.password) 
      if (connTry.isFailure) return Failure(connTry.failed.get)
      conn = connTry.get
      val parseTable = if(sourceJdbcInfo.tableSql.contains(" ")) s"(${sourceJdbcInfo.tableSql})" else sourceJdbcInfo.tableSql
    	val querySql = s"select * from ${parseTable} AAA_BBB_CCC where 1 < 0"
      Try{
        val pstat = conn.createStatement()
        val rsCol = pstat.executeQuery(querySql)
        colNum = rsCol.getMetaData.getColumnCount
        rsCol.close()
        pstat.close()
        ColNum(colNum)
      }
    }
  }
}

object Source extends App{
  def getJdbcConnection(sType:ConnectType, jdbcUrl: String, username: String, password: String): Try[Connection] = {
      val clsDriverOpt = sType match {
        case ConnectType.MYSQL => Some("com.mysql.jdbc.Driver")
        case ConnectType.ORACLE => Some("oracle.jdbc.driver.OracleDriver")
        case ConnectType.HIVE => Some("org.apache.hive.jdbc.HiveDriver")
        case _ => None
      }
      if(clsDriverOpt.isEmpty) return Failure(new Exception(s"不存在该jdbc数据源：${sType}"))
      Try{
    	  //得到连接
  		  Class.forName(clsDriverOpt.get)
    	  DriverManager.getConnection(jdbcUrl, username, password)
      }
    }
  
  
  val si = SourceJdbcInfo(ConnectType.MYSQL, "jdbc:mysql://localhost:3306/wf?useSSL=false", "root", "root", "select name,dir from workflow")
  val s = new Source(si)
  println(s.getColNum())
  println(s.fillRowBuffer())
  s.postStop()
  
}