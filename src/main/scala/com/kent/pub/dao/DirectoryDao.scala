package com.kent.pub.dao

import akka.util.Timeout
import com.kent.workflow.Directory
import scala.concurrent.duration._

/**
  * @author kent
  * @date 2019-04-20
  * @desc
  *
  **/
object DirectoryDao extends TransationManager with Daoable {
  implicit val timeout = Timeout(15 seconds)


  /**
    * 保存叶子节点
    * @param dir
    * @param wfName
    */
  def saveLeafNode(dir: Directory, wfName: String): Unit = {
    transaction {
      val pid = mkdir(dir)
      if (getLeafNode(wfName, pid).isEmpty) {
        delLeafNode(wfName)
        saveLeafNode(wfName, pid)
      }
    }
  }

  def delDir(id: Int): Boolean = {
    transaction{
      val countSql = s"select count(1) cnt from directory where pid = $id"
      val count = query(countSql, rs => {
        if (rs.next()){
          rs.getInt("cnt")
        }else{
          0
        }
      }).get

      if (count > 0){
        false
      } else{
        execute(s"delete from directory where id = $id")
        true
      }
    }
  }

  /**
    * 创建目录，并且返回目录的id
    * @param dir
    * @return
    */
  private def mkdir(dir: Directory): Int = {
    transaction {
      val dirLevels = dir.dirname.split("/").filter { x => x.trim() != "" }.map(_.trim)
      var pid = -1
      for (dir: String <- dirLevels) {
        val result = getFolder(dir, pid)
        if (result.isEmpty) {
          saveFolder(dir, pid)
        }
        pid = getFolder(dir, pid).get
      }
      pid
    }
  }


  private def getFolder(name: String, pid: Int):Option[Int] = {
    import com.kent.util.Util._
    transaction {
      val sql =
        s"""
      select id from directory
      where name=${withQuate(name)} and pid=${pid} and is_leaf=0
    """
      query[String](sql, rs => {
        if (rs.next()) rs.getString("id") else null
      }).map(_.toInt)
    }
  }
  private def saveFolder(name: String, pid: Int):Boolean = {
    import com.kent.util.Util._
    transaction {
      val insertSql = s"insert into directory values(null,${pid},0,${withQuate(name)},null)"
      execute(insertSql)
    }
  }
  private def getLeafNode(name: String, pid: Int): Option[Int] = {
    import com.kent.util.Util._
    transaction {
      val sql =
        s"""
      select id
      from directory
      where name=${withQuate(name)} and pid=${pid} and is_leaf=1
    """
      query[String](sql, rs => {
        if (rs.next()) rs.getString("id") else null
      }).map(_.toInt)
    }
  }
  private def saveLeafNode(name: String, pid: Int):Boolean = {
    import com.kent.util.Util._
    transaction {
      val insertSql = s"insert into directory values(null,${pid},1,${withQuate(name)},null)"
      execute(insertSql)
    }
  }
  private def delLeafNode(name: String):Boolean = {
    import com.kent.util.Util._
    transaction {
      val delSql = s"delete from directory where name = ${withQuate(name)} and is_leaf = 1"
      execute(delSql)
    }
  }

}
