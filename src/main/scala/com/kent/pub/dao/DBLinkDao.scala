package com.kent.pub.dao

import com.kent.pub.db.DBLink
import com.kent.pub.db.DBLink.DatabaseType
import com.kent.util.Util
import com.kent.util.Util._
import org.json4s.DefaultFormats
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

/**
  * @author kent
  * @date 2019-04-22
  * @desc
  *
  **/
object DBLinkDao extends TransationManager with Daoable {
  implicit val formats = DefaultFormats

  def findAll(): List[DBLink] = {
    transaction{
      val sql = s"select * from db_link"
      query[List[DBLink]](sql, rs => {
        var list = List[DBLink]()
        while (rs.next()){
          val name = rs.getString("name")
          val dbType = DatabaseType.withName(rs.getString("dbtype"))
          val desc = rs.getString("description")
          val jdbcUrl = rs.getString("jdbc_url")
          val username = rs.getString("username")
          val password = rs.getString("password")

          val propertiesStr = rs.getString("properties")
          val properties = if(propertiesStr != null) Util.str2Json(propertiesStr).extract[Map[String, String]] else Map[String, String]()

          list = list :+ DBLink(dbType, name, jdbcUrl, username, password, desc, properties)
        }
        list
      }).get
    }
  }

  def save(dbl: DBLink): Boolean = {
    val propertiesStr = compact(render(dbl.properties))

    val sql = s"insert into db_link values(${wq(dbl.name)},${wq(dbl.dbType.toString)}," +
      s"${wq(dbl.description)},${wq(propertiesStr)},${wq(dbl.jdbcUrl)},${wq(dbl.username)}," +
      s"${wq(dbl.password)},${wq(formatStandardTime(nowDate))},${wq(formatStandardTime(nowDate))})"
    execute(sql)
  }

  def get(name: String): Option[DBLink] = {
    val sql = s"select * from db_link where name = ${wq(name)}"
    query[DBLink](sql, rs => {
      if (rs.next()){
        val name = rs.getString("name")
        val dbType = DatabaseType.withName(rs.getString("dbtype"))
        val desc = rs.getString("description")
        val jdbcUrl = rs.getString("jdbc_url")
        val username = rs.getString("username")
        val password = rs.getString("password")

        val propertiesStr = rs.getString("properties")
        val properties = if(propertiesStr != null) Util.str2Json(propertiesStr).extract[Map[String, String]] else Map[String, String]()

        DBLink(dbType, name, jdbcUrl, username, password, desc, properties)
      } else {
        null
      }
    })
  }

  def update(dbl: DBLink): Boolean = {
    val propertiesStr = compact(render(dbl.properties))

    val sql =
      s"""
         update db_link set dbtype = ${wq(dbl.dbType.toString)},
                            description = ${wq(dbl.description)},
                            jdbc_url = ${wq(dbl.jdbcUrl)},
                            username = ${wq(dbl.username)},
                            password = ${wq(dbl.password)},
                            last_update_time = ${wq(formatStandardTime(nowDate))},
                            properties = ${wq(propertiesStr)}
         where name = ${wq(dbl.name)}
       """
    execute(sql)
  }

  def isExistsWithName(name: String): Boolean = {
    val sql = s"select name from db_link where name = ${wq(name)}"
    query[Boolean](sql, rs => {
      if(rs.next()){
        true
      }else{
        false
      }
    }).get
  }

  def delete(name: String): Boolean = {
    val sql = s"delete from db_link where name = ${wq(name)}"
    execute(sql)
  }

}
