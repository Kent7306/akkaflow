package com.kent.pub.dao

import com.kent.pub.db.DBLink
import com.kent.pub.db.DBLink.DatabaseType
import com.kent.pub.io.FileLink
import com.kent.pub.io.FileLink.FileSystemType
import com.kent.util.Util._

/**
  * @author kent
  * @date 2019-04-22
  * @desc
  *
  **/
object FileLinkDao extends TransationManager with Daoable {


  def merge(fl: FileLink) = {
    transaction{
      if (isExistsWithName(fl.name)){
        update(fl)
      }else {
        save(fl)
      }
    }
  }

  def findAll(): List[FileLink] = {
    transaction{
      val sql = s"select * from file_link"
      query[List[FileLink]](sql, rs => {
        var list = List[FileLink]()
        while (rs.next()){
          val name = rs.getString("name")
          val fstype = FileSystemType.withName(rs.getString("fstype"))
          val desc = rs.getString("description")
          val host = rs.getString("host")
          val port = rs.getInt("port")
          val username = rs.getString("username")
          val password = rs.getString("password")
          list = list :+ FileLink(fstype, name, host, port, username, password, desc)
        }
        list
      }).get
    }
  }

  def save(fl: FileLink): Boolean = {
    val sql = s"""insert into file_link values(${wq(fl.name)},${wq(fl.fsType.toString)},${wq(fl.host)},${fl.port},${wq(fl.username)},${wq(fl.password)},${wq(fl.description)})"""
    execute(sql)
  }

  def get(name: String): Option[FileLink] = {
    val sql = s"select * from file_link where name = ${wq(name)}"
    query[FileLink](sql, rs => {
      if (rs.next()){
        val name = rs.getString("name")
        val fstype = FileSystemType.withName(rs.getString("fstype"))
        val desc = rs.getString("description")
        val host = rs.getString("host")
        val port = rs.getInt("port")
        val username = rs.getString("username")
        val password = rs.getString("password")
        FileLink(fstype, name, host, port, username, password, desc)
      } else {
        null
      }
    })
  }

  def update(fl: FileLink): Boolean = {
    val sql =
      s"""
         update file_link set fstype = ${wq(fl.fsType.toString)},
                            description = ${wq(fl.description)},
                            host = ${wq(fl.host)},
                            port = ${fl.port},
                            username = ${wq(fl.username)},
                            password = ${wq(fl.password)}
         where name = ${wq(fl.name)}
       """
    execute(sql)
  }

  def isExistsWithName(name: String): Boolean = {
    val sql = s"select name from file_link where name = ${wq(name)}"
    query[Boolean](sql, rs => {
      if(rs.next()){
        true
      }else{
        false
      }
    }).get
  }

  def delete(name: String): Boolean = {
    val sql = s"delete from file_link where name = ${wq(name)}"
    execute(sql)
  }

}
