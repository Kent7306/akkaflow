package com.kent.pub.dao

import com.kent.daemon.DbConnector
import com.kent.pub.db.DBLink
import com.kent.pub.db.DBLink.DatabaseType
import com.kent.util.Util
import com.kent.util.Util._

/**
  * @author kent
  * @date 2019-04-22
  * @desc
  *
  **/
object DBLinkDao {

  def findAll(): List[DBLink] = {
    val sql = s"select * from db_link"
    DbConnector.querySyn[List[DBLink]](sql, rs => {
      var list = List[DBLink]()
      while (rs.next()){
        val name = rs.getString("name")
        val dbType = DatabaseType.withName(rs.getString("dbtype"))
        val desc = rs.getString("description")
        val jdbcUrl = rs.getString("jdbc_url")
        val username = rs.getString("username")
        val password = rs.getString("password")
        list = list :+ DBLink(dbType, name, jdbcUrl, username, password, desc)
      }
      list
    }).get
  }

  def save(dbl: DBLink): Boolean = {
    val sql = s"insert into db_link values(${wq(dbl.name)},${wq(dbl.dbType.toString)}," +
      s"${wq(dbl.description)},${wq(dbl.jdbcUrl)},${wq(dbl.username)}," +
      s"${wq(dbl.password)},${wq(formatStandarTime(nowDate))},${wq(formatStandarTime(nowDate))})"
    DbConnector.executeSyn(sql)
  }

  def get(name: String): Option[DBLink] = {
    val sql = s"select * from db_link where name = ${wq(name)}"
    DbConnector.querySyn[DBLink](sql, rs => {
      if (rs.next()){
        val name = rs.getString("name")
        val dbType = DatabaseType.withName(rs.getString("dbtype"))
        val desc = rs.getString("description")
        val jdbcUrl = rs.getString("jdbc_url")
        val username = rs.getString("username")
        val password = rs.getString("password")
        DBLink(dbType, name, jdbcUrl, username, password, desc)
      } else {
        null
      }
    })
  }

  def update(dbl: DBLink): Boolean = {
    val sql =
      s"""
         update db_link set dbtype = ${wq(dbl.dbType.toString)},
                            description = ${wq(dbl.description)},
                            jdbc_url = ${wq(dbl.jdbcUrl)},
                            username = ${wq(dbl.username)},
                            password = ${wq(dbl.password)},
                            last_update_time = ${wq(formatStandarTime(nowDate))}
         where name = ${wq(dbl.name)}
       """
    DbConnector.executeSyn(sql)
  }

  def isExistsWithName(name: String): Boolean = {
    val sql = s"select name from db_link where name = ${wq(name)}"
    DbConnector.querySyn[Boolean](sql, rs => {
      if(rs.next()){
        true
      }else{
        false
      }
    }).get
  }

  def delete(name: String): Boolean = {
    val sql = s"delete from db_link where name = ${wq(name)}"
    DbConnector.executeSyn(sql)
  }

}
