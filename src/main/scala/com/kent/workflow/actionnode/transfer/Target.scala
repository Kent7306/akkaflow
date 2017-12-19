package com.kent.workflow.actionnode.transfer

import akka.actor.ActorLogging
import akka.actor.Actor
import com.kent.pub.Event.Start
import com.kent.workflow.actionnode.TransferNode.SourceInfo
import akka.actor.ActorRef
import scala.concurrent.ExecutionContext.Implicits.global
import com.kent.workflow.actionnode.TransferNode._
import com.kent.workflow.actionnode.TransferNode.ConnectType._
import akka.pattern.{ ask, pipe }
import scala.concurrent.duration._
import akka.util.Timeout
import scala.util.Try
import scala.concurrent.Future
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogReader
import com.kent.db.LogRecorder
import com.kent.db.LogRecorder.LogType
import com.kent.db.LogRecorder.LogType._
import java.sql._
import scala.util.Failure
import scala.util.Success
import java.io.File
import java.io.FileWriter
import java.io.BufferedWriter

class Target(tarInfo: TargetInfo,actionName: String,wfiId: String, sourceRef: ActorRef) extends Actor with ActorLogging 
{
  implicit val timeout = Timeout(20 seconds)
  
  private var conn: Connection = null
  private var colNum: Int = 0
  private var isReadEnd = false
  
  private var controlRef: ActorRef = _
  
  private var counter: Int = 0
  
  override def preStart(): Unit = {  
    
  }
  override def postStop(): Unit = {
    if(conn != null) conn.close()
  }
  
  def receive: Actor.Receive = {
    case Start() =>  start(sender)
    case Success(Rows(rows)) => persistRows(rows)
    case Failure(e) => 
      LogRecorder.error(LogType.ACTION_NODE_INSTANCE, wfiId, actionName, e.getMessage)
      controlRef ! false
      context.stop(self)
  }
  
  def start(sdr: ActorRef) = {
    controlRef = sender;
    checkColNumAndPrepare().map{
      case x if x == true => sourceRef ! GetRows()
      case x if x == false => 
        controlRef ! false
        context.stop(self)
    }
  }
  
  def persistRows(rows:List[List[String]]): Unit = {
    if(rows.filter { _ == null }.size == 0) sourceRef ! GetRows()
    counter += rows.filter { _ != null }.size
    LogRecorder.info(LogType.ACTION_NODE_INSTANCE, wfiId, actionName, s"已传输${counter}条数据")
    if(tarInfo.sType == ConnectType.LFS){
      val tarFileInfo = tarInfo.asInstanceOf[TargetFileInfo]
      var out: FileWriter = null
      var bw: BufferedWriter = null
      try {
        out = new FileWriter(tarFileInfo.path, true) 
        bw = new BufferedWriter(out)
        rows.foreach { 
          case cols if cols != null => 
            bw.write(cols.mkString(tarFileInfo.delimited)+"\n")
          case cols if cols == null => isReadEnd = true
        }
        bw.flush()
        if(isReadEnd){
          controlRef ! true
          context.stop(self)
        }
      } catch{
        case e: Exception => 
          e.printStackTrace()
          LogRecorder.info(LogType.ACTION_NODE_INSTANCE, wfiId, actionName, e.getMessage)
          controlRef ! false
          context.stop(self)
      } finally {
        if(bw != null) bw.close()
        if(out != null) out.close()
      }
    }else{
      val tarJdbcInfo = tarInfo.asInstanceOf[TargetJdbcInfo]
      val paramLine = (1 to this.colNum).map{x => "?"}.mkString(",")
      val insertSql = s"insert into ${tarJdbcInfo.table} values(${paramLine})"
      var pstat: PreparedStatement = null
      try {
        val connTry = getConnection(tarJdbcInfo)
        if(connTry.isFailure) throw connTry.failed.get
        val connection = connTry.get
        connection.setAutoCommit(false)
        pstat = conn.prepareStatement(insertSql)
        rows.foreach { 
          case cols if cols == null => isReadEnd = true
          case cols if cols != null =>
          (1 to cols.size).foreach{ idx => pstat.setString(idx, cols(idx-1)) }
          pstat.addBatch()
        }
        pstat.executeBatch()
        connection.commit()
        if(isReadEnd){
          //执行后置sql，若都成功才算成功。
          val afterSqls = if(tarJdbcInfo.afterSql != null) tarJdbcInfo.afterSql.split(";").map(_.trim()).toList else List()
        	executeSqls(afterSqls, conn) match {
        	  case Success(result) => controlRef ! result
        	  case Failure(e) => 
        	    LogRecorder.error(LogType.ACTION_NODE_INSTANCE, wfiId, actionName, "后置sql执行出错："+e.getMessage)
        	    controlRef ! false
        	}
        	context.stop(self)
        }
      } catch{
        case e: Exception => 
          e.printStackTrace()
          LogRecorder.error(LogType.ACTION_NODE_INSTANCE, wfiId, actionName, e.getMessage)
          controlRef ! false
          context.stop(self)
      } finally {
        pstat.close()
      }
    }
  }
  
  def preClear():Try[Boolean] = {
    if(tarInfo.sType == ConnectType.LFS){
      val tarFileInfo = tarInfo.asInstanceOf[TargetFileInfo]
      if(tarFileInfo.isPreDel){
    	  val f = new File(tarFileInfo.path)
    	  val result = if(f.exists()) f.delete() else true
    	  if(!result) LogRecorder.error(LogType.ACTION_NODE_INSTANCE, wfiId, actionName, s"${tarFileInfo.path}删除失败")
        Success(result)
      }else{
    	  Success(true)        
      }
    }else{
      val tarJdbcInfo = tarInfo.asInstanceOf[TargetJdbcInfo]
      val clearSql = if(tarJdbcInfo.isPreTruncate) s"delete from ${tarJdbcInfo.table} where 1 = 1" else null
      val preSqls = if(tarJdbcInfo.preSql != null) tarJdbcInfo.preSql.split(";").map(_.trim()).toList else List()
      var sqls = List(clearSql) ++ preSqls
      getConnection(tarJdbcInfo).map { conTmp => executeSqls(preSqls, conTmp) }.flatten
    }
  }
  
  private def executeSqls(sqls: List[String], conn: Connection):Try[Boolean] = {
    val pSql = if(sqls != null) sqls.filter {x => x != null && x.trim() != "" }.toList else List[String]()
    Try{
    	if(pSql.size == 0) { 
    	  true
    	}else {
        conn.setAutoCommit(false)
        val stat = conn.createStatement()
        pSql.map(stat.execute(_))
        conn.commit()
        stat.close()
        true
    	}
    }
  }
  
  def checkColNumAndPrepare(): Future[Boolean] = {
    //判断源与目标字段是否相等
    val colNumTryF = (sourceRef ? GetColNum()).mapTo[Try[ColNum]]
    val checkTryF = colNumTryF.map { colNumTry => 
      colNumTry.map { scn => this.getColNum().map { 
        case colNum if(colNum == -1 || scn.colNum == colNum) => true
        case colNum => 
          LogRecorder.error(LogType.ACTION_NODE_INSTANCE, wfiId, actionName, s"源数据字段（${scn.colNum}）与目标表的字段（${colNum}）不等")
          false
        } 
      }.flatten
    }
    //执行插入前清理，及处理异常
    checkTryF.map { checkTry => 
      val tmp = checkTry.map { check => if(check) preClear() else Success(check) }.flatten
      if(tmp.isFailure) {
        tmp.failed.get.printStackTrace()
        LogRecorder.error(LogType.ACTION_NODE_INSTANCE, wfiId, actionName, tmp.failed.get.getMessage)
        false
      }else tmp.get
    }
  }
  /**
   * 获取字段个数，如果目标是文件，则返回-1，表示不用判断，其余正常返回字段个数
   * 
   */
  def getColNum():Try[Int] = {
    if(tarInfo.sType == ConnectType.LFS){
      Success(-1)
    }else{
    	val tarJdbcInfo = tarInfo.asInstanceOf[TargetJdbcInfo]
    	val connOpt = getConnection(tarJdbcInfo)
    	connOpt.map { x => 
    	    conn = x
    			val stat = x.createStatement()
    			val rs = stat.executeQuery(s"select * from ${tarJdbcInfo.table} where 1 > 2")
    			this.colNum = rs.getMetaData.getColumnCount
    			rs.close()
    			stat.close()
    			colNum
    	}
    }
  }
  /**
   * 获取数据库连接
   */
  def getConnection(tarJdbcInfo: TargetJdbcInfo):Try[Connection] = {
    if(conn != null) 
      Success(conn) 
    else {
      val connTry = Source.getJdbcConnection(tarJdbcInfo.sType, tarJdbcInfo.jdbcUrl, tarJdbcInfo.username, tarJdbcInfo.password)
      connTry.map{conn = _}
      connTry
    }
  }
}

object Target extends App{
  //val t = TargetJdbcInfo(ConnectType.MYSQL,"jdbc:mysql://localhost:3306/test?useSSL=false", "root", "root",true,"aaa", null, null)
	val t = TargetFileInfo(ConnectType.LFS, "/tmp/111", true, "***")
  val a = new Target(t,"1111","2222",null)
  val l = List(List("1122"),List("22444"))
  val b = a.checkColNumAndPrepare().map {case x => a.persistRows(l) }
  Thread.sleep(3000)
}