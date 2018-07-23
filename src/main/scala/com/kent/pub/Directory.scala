package com.kent.pub

import java.sql.Connection
import java.sql.DriverManager

/**
 * coordinator或workflow的目录类
 * dirname: 目录
 * dtype: 0 -> coordinator, 1 -> workflow
 */
class Directory(val dirname: String) extends Persistable[Directory] with DeepCloneable[Directory]{
  private val dirLevels = dirname.split("/").filter { x => x.trim() != "" }.toList
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
  def saveLeafNode(nodeName: String)(implicit conn: Connection): Boolean = {
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
      from directory 
      where name=${withQuate(name)} and pid=${pid} and is_leaf=0
    """
    val result = querySql(sql, rs => {
      if (rs.next()) rs.getString("id") else null
    })
    if(result.isEmpty) None else Some(result.get.toInt)
  }
  private def getLeafNode(name: String, pid: Int)(implicit conn: Connection):Option[Int] = {
    import com.kent.util.Util._
    val sql = s"""
      select id 
      from directory 
      where name=${withQuate(name)} and pid=${pid} and is_leaf=1
    """
    val result = querySql(sql, rs => {
      if (rs.next()) rs.getString("id") else null
    })
    if(result.isEmpty) None else Some(result.get.toInt)
  }
  private def saveFolder(name: String, pid: Int)(implicit conn: Connection):Boolean = {
    import com.kent.util.Util._
    val insertSql = s"insert into directory values(null,${pid},0,${withQuate(name)},null)"
    //println(insertSql);
    executeSql(insertSql)
  }
  private def saveLeafNode(name: String, pid: Int)(implicit conn: Connection):Boolean = {
    import com.kent.util.Util._
    val insertSql = s"insert into directory values(null,${pid},1,${withQuate(name)},null)"
    executeSql(insertSql)
  }
  private def delLeafNode(name: String)(implicit conn: Connection):Boolean = {
    import com.kent.util.Util._
    val delSql = s"delete from directory where name = ${withQuate(name)} and is_leaf = 1"
    executeSql(delSql)
  }
  

}

object Directory extends App{
  def apply(dirname: String): Directory = new Directory(dirname)
  val a = Directory("/home//2222/")
  val b = Directory("/home///111/")
  
  implicit var connection: Connection = null
  Class.forName("com.mysql.jdbc.Driver")
  connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/wf?useSSL=false", "root", "root")
  a.saveLeafNode("wf_join")
  b.saveLeafNode("coordinatxxxx")
  connection.close()
}














