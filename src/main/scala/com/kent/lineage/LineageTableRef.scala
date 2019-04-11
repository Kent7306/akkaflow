package com.kent.lineage

import java.sql.Connection

import com.kent.pub.{DeepCloneable, Persistable}

class LineageTableRef  extends DeepCloneable[LineageTableRef] with Persistable[LineageTableRef] {
  var tableName: String = _
  var sourceTableNames:List[String] = _
  
  import com.kent.util.Util._
  def delete(implicit conn: Connection): Boolean = {
    val delSql = s"delete from lineage_table_ref where pname = ${wq(tableName)}"
    executeSql(delSql)
  }

  def getEntity(implicit conn: Connection): Option[LineageTableRef] = {
    val existSql = s"select * from lineage_table_ref where pname = ${wq(tableName)}"
    val slistOpt = querySql(existSql, rs => {
      val slist = scala.collection.mutable.Buffer[String]()
      while(rs.next()){
        slist += rs.getString("name")
      }
      slist.toList
    })
    if(slistOpt.isEmpty){
      None
    }else{
      Some(LineageTableRef(tableName, slistOpt.get))
    }
  }

  def save(implicit conn: Connection): Boolean = {
    
    val insertSqls = sourceTableNames.map{ stable =>
      s"insert into lineage_table_ref values(null,${wq(stable)},${wq(tableName)})"
    }
    this.delete
    executeSqls(insertSqls)
  }
}

object LineageTableRef {
  def apply(tableName: String, sourceTableNames: List[String]):LineageTableRef = {
    val tr = new LineageTableRef()
    tr.tableName = tableName
    tr.sourceTableNames = sourceTableNames
    tr
  }
}