package com.kent.workflow.node.action

import akka.actor.Props
import akka.pattern.{ask, pipe}

import scala.concurrent.duration._
import com.kent.pub.Event.Start
import akka.util.Timeout

import scala.concurrent.Await
import akka.actor.ActorRef

import scala.sys.process._
import com.kent.workflow.node.action.transfer.source._
import com.kent.workflow.node.action.transfer.{Consumer, DataShare, Producer, Reader, TransferPoison, Writer}
import com.kent.workflow.node.action.transfer.target._
import com.kent.workflow.node.action.transfer.target.Target
import com.kent.pub.db.DBLink.DatabaseType._
import com.kent.pub.io.FileLink.FileSystemType
import com.kent.util.Util
import com.kent.workflow.node.action.transfer.source.db.{HdfsSource, LocalFileSource, SFtpSource}
import com.kent.workflow.node.action.transfer.source.io.{HiveSource, MysqlSource, OracleSource}
import com.kent.workflow.node.action.transfer.target.db.{HdfsTarget, LocalFileTarget, SFtpTarget}
import com.kent.workflow.node.action.transfer.target.io.{HiveTarget, MysqlTarget, OracleTarget}

class TransferNodeInstance(override val nodeInfo: TransferNode) extends ActionNodeInstance(nodeInfo) {  
  var ds: DataShare = _
  private var executeProcess: Process = _
  
  def execute(): Boolean = {
    if(nodeInfo.script.isEmpty){
      executeActorTransfer()
    }else{
      executeScript(nodeInfo.script.get, None){x => this.executeProcess = x}
    }
  }

  /**
    * 使用actor进行导数
    * @return
    */
  def executeActorTransfer():Boolean = {
    val targetTaskNum = getTargetTaskNum()
    ds = new DataShare(targetTaskNum)
    val source = getSource()
    val reader = new Reader(source, ds, "reader")
    infoLog(s"同步到目标，将启动${targetTaskNum}个线程")
    val targets = (0 until targetTaskNum).map{_ => getTarget()}
    val writers = targets.zipWithIndex.map{case(t,idx) => new Writer(t, ds, s"writer-$idx")}

    val sourceCols = source.doGetColumns.get
    //设置所有target的字段集合
    val targetCol = targets.head.initColWithSourceCols(sourceCols)
    targets.foreach(x => x.columns = targetCol)
    //前置sql执行
    targets.head.preOpera()
    val stime = Util.nowTime
    reader.start()
    writers.foreach(_.start())
    reader.join()
    writers.foreach(_.join())
    //后置sql执行
    if (ds.exceptionOpt.isEmpty) {
      targets.head.afterOpera()
      val period = ((Util.nowTime - stime)/1000).toInt
      val avgInsertCnt = (ds.getPersistCnt().toFloat/(Util.nowTime - stime)*1000).toInt
      infoLog(s"同步结束，耗时${period}s,一共插入${ds.getPersistCnt()}条数据，平均每秒插入${avgInsertCnt}条")
    } else {
      throw ds.exceptionOpt.get
    }

    true
  }

  /**
    * 获取源对象
    * @return
    */
  private def getSource(): Source = {
    val source: Source = if(nodeInfo.dbsInfOpt.isDefined){  //DB source
      //获取dblink信息
      val dbLinkF = actionActor.getDBLink(nodeInfo.dbsInfOpt.get.dbLinkName)
      val dbLinkOpt = Await.result(dbLinkF, 20 seconds)
      //找到对应的数据库连接源
      dbLinkOpt match {
        case Some(dbl) if dbl.dbType == MYSQL => new MysqlSource(dbLinkOpt.get, nodeInfo.dbsInfOpt.get.query)
        case Some(dbl) if dbl.dbType == ORACLE => new OracleSource(dbLinkOpt.get, nodeInfo.dbsInfOpt.get.query)
        case Some(dbl) if dbl.dbType == HIVE => new HiveSource(dbLinkOpt.get, nodeInfo.dbsInfOpt.get.query)
        case None =>  throw new Exception(s"source中未找到对应的db-link配置: ${nodeInfo.dbsInfOpt.get.dbLinkName}")
      }
    } else if (nodeInfo.fsInfOpt.isDefined){   //file source
      //获取fileLink信息
      val flOptF = actionActor.getFileLink(nodeInfo.fsInfOpt.get.fileLinkName)
      val flOpt = Await.result(flOptF, 20 seconds)
      val fsInf = nodeInfo.fsInfOpt.get
      //找到对应的文件系统连接源
      flOpt match {
        case Some(fl) if fl.fsType == FileSystemType.LOCAL => new LocalFileSource(fl, fsInf.delimited, fsInf.path)
        case Some(fl) if fl.fsType == FileSystemType.HDFS => new HdfsSource(fl, fsInf.delimited, fsInf.path)
        case Some(fl) if fl.fsType == FileSystemType.SFTP => new SFtpSource(fl, fsInf.delimited, fsInf.path)
        case None => throw new Exception(s"source中未找到对应的file-link配置: ${nodeInfo.fsInfOpt.get.fileLinkName}")
      }
    } else {
      throw new Exception("source中未配置db-link或file-link")
    }
    if(source == null) throw new Exception("未找到相应的source")
    source.actionName = this.nodeInfo.name
    source.instanceId = this.id
    source
  }

  /**
    * 获取目标对象
    * @return
    */
  private def getTarget(): Target = {
    val target: Target = if(nodeInfo.dbtInfOpt.isDefined){  //DB target
      //获取数据库连接
      val dbtInf = nodeInfo.dbtInfOpt.get
      val dbLinkF = actionActor.getDBLink(dbtInf.dbLinkName)
      val dbLinkOpt = Await.result(dbLinkF, 20 seconds)

      dbLinkOpt match {
        case Some(dbl) if dbl.dbType == MYSQL => new MysqlTarget(nodeInfo.name, id, dbtInf.isPreTruncate, dbLinkOpt.get, dbtInf.table, dbtInf.preSql, dbtInf.afterSql)
        case Some(dbl) if dbl.dbType == ORACLE => new OracleTarget(nodeInfo.name, id, dbtInf.isPreTruncate, dbLinkOpt.get, dbtInf.table, dbtInf.preSql, dbtInf.afterSql)
        case Some(dbl) if dbl.dbType == HIVE => {
          //获取fileLink信息
          val flsF = actionActor.getAllFileLink()
          val fls = Await.result(flsF, 20 seconds)
          val fl = fls.find(_.fsType == FileSystemType.HDFS) match {
            case Some(f) => f
            case None => throw new Exception("hive target必须要先配置HDFS类型的文件接入系统")
          }

          new HiveTarget(nodeInfo.name, id, dbtInf.isPreTruncate, dbLinkOpt.get, fl, dbtInf.table, dbtInf.preSql, dbtInf.afterSql)
        }
        case None => throw new Exception(s"target中未找到对应的db-link配置: ${dbtInf.dbLinkName}")
      }
    } else if(nodeInfo.ftInfOpt.isDefined){  //HDFS target
      //获取fileLink信息
      val flOptF = actionActor.getFileLink(nodeInfo.ftInfOpt.get.fileLinkName)
      val flOpt = Await.result(flOptF, 20 seconds)
      val ftInf = nodeInfo.ftInfOpt.get
      //找到对应的文件系统连接源
      flOpt match {
        case Some(fl) if fl.fsType == FileSystemType.LOCAL => new LocalFileTarget(flOpt.get, ftInf.delimited,ftInf.path,ftInf.isPreDel, ftInf.preCmd, ftInf.afterCmd, id, nodeInfo.name,this)
        case Some(fl) if fl.fsType == FileSystemType.HDFS => new HdfsTarget(flOpt.get, ftInf.delimited,ftInf.path,ftInf.isPreDel, ftInf.preCmd, ftInf.afterCmd, id, nodeInfo.name,this)
        case Some(fl) if fl.fsType == FileSystemType.SFTP => new SFtpTarget(flOpt.get, ftInf.delimited,ftInf.path,ftInf.isPreDel, ftInf.preCmd, ftInf.afterCmd, id, nodeInfo.name,this)
        case None => throw new Exception("target中未找到对应的file-link配置")
      }
    } else {
      throw new Exception("target中未配置db-link或file-link")
    }
    if(target == null) throw new Exception("target未找到相应配置")
    target
  }

  /**
    * 获取目标子任务数
    * @return
    */
  private def getTargetTaskNum(): Int = {
    if(nodeInfo.dbtInfOpt.isDefined) { //DB target
      nodeInfo.dbtInfOpt.get.taskNum
    } else if(nodeInfo.ftInfOpt.isDefined){  //HDFS target
      nodeInfo.ftInfOpt.get.taskNum
    } else {
      throw new Exception("target中未配置db-link或file-link")
    }
  }

  def kill(): Boolean = {
    if(ds != null) {
      ds.put(TransferPoison())
    }
    if(executeProcess != null) executeProcess.destroy()
    true
  }
}