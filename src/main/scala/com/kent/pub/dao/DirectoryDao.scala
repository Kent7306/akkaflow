package com.kent.pub.dao

import akka.util.Timeout
import com.kent.daemon.DbConnector
import com.kent.workflow.Directory
import scala.concurrent.duration._

/**
  * @author kent
  * @date 2019-04-20
  * @desc
  *
  **/
object DirectoryDao {
  implicit val timeout = Timeout(15 seconds)


  /**
    * 保存叶子节点
    */
  def saveLeafNode(dir: Directory, wfName: String): Unit = {
    val pid = mkdir(dir)
    if(getLeafNode(wfName, pid).isEmpty){
      delLeafNode(wfName)
      saveLeafNode(wfName, pid)
    }
  }
  /**
    * 创建目录，并且返回目录的id
    * @param dir
    * @return
    */
  private def mkdir(dir: Directory): Int = {
    val dirLevels = dir.dirname.split("/").filter { x => x.trim() != "" }.map(_.trim)
    var pid = -1
    for(dir: String <- dirLevels){
      val result = getFolder(dir, pid)
      if(result.isEmpty){
        saveFolder(dir, pid)
      }
      pid = getFolder(dir, pid).get
    }
    pid
  }


  private def getFolder(name: String, pid: Int):Option[Int] = {
    import com.kent.util.Util._
    val sql = s"""
      select id from directory
      where name=${withQuate(name)} and pid=${pid} and is_leaf=0
    """
    DbConnector.querySyn[Int](sql, rs => {
      if (rs.next()) rs.getString("id").toInt else null
    })
  }
  private def saveFolder(name: String, pid: Int):Boolean = {
    import com.kent.util.Util._
    val insertSql = s"insert into directory values(null,${pid},0,${withQuate(name)},null)"
    DbConnector.executeSyn(insertSql)
  }
  private def getLeafNode(name: String, pid: Int): Option[Int] = {
    import com.kent.util.Util._
    val sql = s"""
      select id
      from directory
      where name=${withQuate(name)} and pid=${pid} and is_leaf=1
    """
    DbConnector.querySyn[Int](sql, rs => {
      if (rs.next()) rs.getString("id") else null
    })
  }
  private def saveLeafNode(name: String, pid: Int):Boolean = {
    import com.kent.util.Util._
    val insertSql = s"insert into directory values(null,${pid},1,${withQuate(name)},null)"
    DbConnector.executeSyn(insertSql)
  }
  private def delLeafNode(name: String):Boolean = {
    import com.kent.util.Util._
    val delSql = s"delete from directory where name = ${withQuate(name)} and is_leaf = 1"
    DbConnector.executeSyn(delSql)
  }

}
