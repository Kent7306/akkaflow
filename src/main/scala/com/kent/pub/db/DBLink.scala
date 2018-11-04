package com.kent.pub.db

import com.kent.pub.db.DBLink.DatabaseType.DatabaseType
import com.kent.pub.DeepCloneable

class DBLink() extends DeepCloneable[DBLink]{
  var dbType: DatabaseType = _
  var name: String = _
  var jdbcUrl: String = _
  var username: String = _
  var password: String = _
}

object DBLink{
  def apply(dbType: DatabaseType, name: String, jdbcUrl: String, username: String, password: String): DBLink = {
    val dbLink = new DBLink()
    dbLink.dbType = dbType
    dbLink.name = name
    dbLink.jdbcUrl = jdbcUrl
    dbLink.username = username
    dbLink.password = password
    dbLink
  }
  
  object DatabaseType extends Enumeration {
    type DatabaseType = Value
    val HIVE, ORACLE, MYSQL = Value
  }
}