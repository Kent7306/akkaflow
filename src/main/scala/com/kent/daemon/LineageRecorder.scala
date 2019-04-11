package com.kent.daemon

import akka.actor.Actor
import akka.pattern.{ask, pipe}
import com.kent.daemon.LineageRecorder.LineageRecord
import com.kent.lineage.{LineageTable, LineageTableRef}
import com.kent.main.Master
import com.kent.pub.Event._
import com.kent.pub.actor.Daemon

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
 * 血缘关系记录器（beta）
 */
class LineageRecorder extends Daemon {
  def individualReceive: Actor.Receive = {
    case SaveLineageRecord(lr) => saveRecord(lr)
    case DelLineageTable(name) => delTable(name) pipeTo sender
  }
  /**
   * 保存血缘关系
   */
  def saveRecord(lr: LineageRecord) =  {
    //保存目标表信息
    Master.persistManager ! Save(lr.targetTable)
    //得到源表信息集合
    val sourceTableFs = lr.relate.sourceTableNames.map{ case st =>
      val tableOptF = (Master.persistManager ? Get(LineageTable(st))).mapTo[Option[LineageTable]]
      tableOptF.map{x => (x, st)}
    }
    Future.sequence(sourceTableFs).onComplete{
      case Success(sTableOpts) =>
        //源表访问+1(如果某源表信息在血缘表中不存在，则新建一条记录)
        sTableOpts.map{
          case (Some(x),_) => x
          case (None, y) => 
            val t = LineageTable(y)
            t.dbLinkName = lr.targetTable.dbLinkName
            t.workflowName = lr.targetTable.dbLinkName
            t
        }.foreach{ x =>
          x.accessNum = x.accessNum + 1
          Master.persistManager ! Save(x)
        }
        Master.persistManager ! Save[LineageTableRef](lr.relate)
      case Failure(e) => ???
    }
  }
  /**
   * 删除血缘关系中某个表
   */
  def delTable(tableName: String): Future[Boolean] = {
    val t = LineageTable(tableName)
    val tr = LineageTableRef(tableName, List())
    for{
     	rs1 <- (Master.persistManager ? Delete[LineageTable](t)).mapTo[Boolean]
     	rs2 <- (Master.persistManager ? Delete[LineageTableRef](tr)).mapTo[Boolean]
    }yield(rs2)
  }
}

object LineageRecorder{
  def apply():LineageRecorder = {
    new LineageRecorder()
  }
  
  case class LineageRecord(targetTable: LineageTable, relate: LineageTableRef)
}



