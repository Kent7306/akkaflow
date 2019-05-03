package com.kent.daemon

import java.sql._

import akka.actor.{Actor, ActorRef}
import com.kent.main.Master
import com.kent.pub.Event._
import com.kent.pub.actor.Daemon

import scala.concurrent.{Await, Future}
import scala.io.Source
import scala.util.Try
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import com.kent.pub.dao.Daoable


/**
  * 数据库连接器，用于元数据以及持久化及查询
  * @param url
  * @param username
  * @param pwd
  */
class DbConnector(url: String, username: String, pwd: String) extends Daemon with Daoable {
  implicit var connection: Connection = _
  //只需要第一次启动时初始化建表sql
  var isInitSqlNeeded = true

  override def preStart() {
    connection = getConnection(url, username, pwd)
    if (isInitSqlNeeded) initSql()
  }

  override def postRestart(reason: Throwable) {
    super.postRestart(reason)
    this.isInitSqlNeeded = false
    log.info(s"${reason.getMessage},pm管理器即将重启...")
  }

  override def individualReceive: Actor.Receive = {
    case ExecuteSqls(sqls) => sender ! doExecuteSqls(sqls)
    case QuerySql(sql, rsHandler) => sender ! doQuerySql(sql, rsHandler)
  }

  /**
    * 执行多条SQL
    * @param sqls
    * @return
    */
  def doExecuteSqls(sqls: List[String]): Result = {
    Try {
      //println("*********")
      //println(sqls)
      //println("----------------")
      this.executeSqls(sqls)
      Result(true, "执行成功", None)
    }.recover {
      case e: SQLException =>
        e.printStackTrace()
        Result(false, e.getMessage, None)
    }.get
  }

  /**
    * 查询SQL，并返回用户自定义的实体
    * @param sql
    * @param rsHandler
    * @return
    */
  def doQuerySql(sql: String, rsHandler: ResultSet => Any): Result = {
    Try {
     // println("*********")
     // println(sql)
     // println("----------------")
      val entityOpt = this.querySql(sql, rsHandler)
      Result(true, "执行成功", entityOpt)
    }.recover {
      case e: Exception =>
        e.printStackTrace()
        Result(false, e.getMessage, None)
    }.get
  }


  private def initSql() = {
    var stat: Statement = null
    if (connection != null) {
      try {
        //启动清理
        var content = ""
        Source.fromFile(this.getClass.getResource("/").getPath + "/create_table.sql").foreach {
          content += _
        }
        val sqls = content.split(";").filter {
          _.trim() != ""
        }.toList
        connection.setAutoCommit(false)
        stat = connection.createStatement()
        sqls.map {
          stat.execute(_)
        }
        connection.commit()
      } catch {
        case e: Exception =>
          e.printStackTrace()
          connection.rollback()
          log.error("执行初始化建表sql失败")
          throw e
      } finally {
        if (stat != null) stat.close()
        connection.setAutoCommit(true)
      }
      log.info(s"${Daemon.DB_CONNECTOR}成功初始化数据库")
    }
  }

  override def postStop() {
    if (connection != null) connection.close()
  }
}

object DbConnector {
  val duration = 15 seconds

  implicit val timeout = Timeout(duration)
  def apply(url: String, username: String, pwd: String):DbConnector = new DbConnector(url, username, pwd)

  var dbConnector: ActorRef = Master.dbConnector

  /**
    * 查询返回某个实体
    * @param sql
    * @param rsHandler
    * @tparam A
    * @return
    */
  def query[A](sql: String, rsHandler: ResultSet => Any): Future[Option[A]] = {
    (dbConnector ? QuerySql(sql, rsHandler)).mapTo[Result].map {
      case r@Result(true, _, _) => r.toDataOpt[A]
      case Result(false, msg, _) => throw new Exception(msg)
    }
  }

  /**
    * 同步查询返回某个实体
    * @param sql
    * @param rsHandler
    * @tparam A
    * @return
    */
  def querySyn[A](sql: String, rsHandler: ResultSet => Any):Option[A] = {
    val resultF = query[A](sql, rsHandler)
    Await.result(resultF, duration)
  }

  /**
    * 执行sqls
    * @param sqls
    * @return
    */
  def execute(sqls: List[String]): Future[Boolean] = {
    (dbConnector ? ExecuteSqls(sqls)).mapTo[Result].map {
      case Result(true, _, _) => true
      case Result(false, msg, _) => throw new Exception(msg)
    }
  }

  def executeSyn(sqls: List[String]): Boolean = {
    val resultF = execute(sqls)
    Await.result(resultF, duration)
  }

  /**
    * 执行sqls
    * @param sql
    * @return
    */
  def execute(sql: String): Future[Boolean] = execute(List(sql))

  def executeSyn(sql: String): Boolean = executeSyn(List(sql))
}
