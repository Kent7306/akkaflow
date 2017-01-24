package com.kent.pub

import java.sql.Connection
import java.sql.DriverManager

/**
 * coordinator或workflow的目录类
 */
class Directory(val dirname: String,val dtype: Int) extends Daoable[Directory] {
  val dirLevels = dirname.split("/").filter { x => x.trim() != "" }.toList
  def delete(implicit conn: Connection): Boolean = {
    ???
  }

  def getEntity(implicit conn: Connection): Option[Directory] = {
    ???
  }
  def save(implicit conn: Connection): Boolean = {
    ???
  }
  /**
   * 保存叶子节点
   */
  def newLeafNode(nodeName: String)(implicit conn: Connection): Boolean = {
    val pid = mkdir(dirname)
    if(getLeafNode(nodeName, pid).isEmpty){
      delLeafNode(nodeName)
      saveLeafNode(nodeName, pid)
    }
    true
  }
  /**
   * 创建目录，并且返回目录的id
   */
  private def mkdir(dirname: String)(implicit conn: Connection):Int = {
    val dirLevels = dirname.split("/").filter { x => x.trim() != "" }.toList
    var pid = -1;
    for(dir: String <- dirLevels){
        val result = getFolder(dir, pid)
        if(result.isEmpty){
          saveFolder(dir, pid)
        }
        pid = getFolder(dir, pid).get
    }
    pid
  }
  private def getFolder(name: String, pid: Int)(implicit conn: Connection):Option[Int] = {
    import com.kent.util.Util._
    val sql = s"""
      select id 
      from directory_info 
      where name=${withQuate(name)} and pid=${pid} and dtype=${dtype} and is_leaf=0
    """
    val result = querySql[String](sql, rs => {
      if (rs.next()) rs.getString("id") else null
    })
    if(result.isEmpty) None else Some(result.get.toInt)
  }
  private def getLeafNode(name: String, pid: Int)(implicit conn: Connection):Option[Int] = {
    import com.kent.util.Util._
    val sql = s"""
      select id 
      from directory_info 
      where name=${withQuate(name)} and pid=${pid} and dtype=${dtype} and is_leaf=1
    """
    val result = querySql[String](sql, rs => {
      if (rs.next()) rs.getString("id") else null
    })
    if(result.isEmpty) None else Some(result.get.toInt)
  }
  private def saveFolder(name: String, pid: Int)(implicit conn: Connection):Boolean = {
    import com.kent.util.Util._
    val insertSql = s"insert into directory_info values(null,${pid},0,${dtype},${withQuate(name)},null)"
    executeSql(insertSql)
  }
  private def saveLeafNode(name: String, pid: Int)(implicit conn: Connection):Boolean = {
    import com.kent.util.Util._
    val insertSql = s"insert into directory_info values(null,${pid},1,${dtype},${withQuate(name)},null)"
    executeSql(insertSql)
  }
  private def delLeafNode(name: String)(implicit conn: Connection):Boolean = {
    import com.kent.util.Util._
    val delSql = s"delete from directory_info where name = ${withQuate(name)} and is_leaf = 1 and dtype = ${dtype}"
    executeSql(delSql)
  }
  

}

object Directory extends App{
  def apply(dirname: String,dtype: Int): Directory = new Directory(dirname, dtype)
  val a = Directory("/home//2222/",0)
  val b = Directory("/home///111/",1)
  
  implicit var connection: Connection = null
  Class.forName("com.mysql.jdbc.Driver")
  connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/wf?useSSL=false", "root", "root")
  a.newLeafNode("wf_join")
  b.newLeafNode("coordinatxxxx")
  connection.close()
}














