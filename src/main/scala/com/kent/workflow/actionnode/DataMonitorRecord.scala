package com.kent.workflow.actionnode

import com.kent.pub.Daoable
import java.sql.Connection
import java.sql.ResultSet
import com.kent.pub.DeepCloneable
import java.sql.SQLException
import com.kent.pub.Persistable

class DataMonitorRecord(val timeMark: String, val category: String, val sourceName: String, val num:Double, val minOpt: Option[Double], val maxOpt: Option[Double],val remark: String, val wfiId: String) 
       extends Persistable[DataMonitorRecord] with DeepCloneable[DataMonitorRecord] {
  def delete(implicit conn: Connection): Boolean = {
    import com.kent.util.Util._
    executeSql(s"""delete from data_monitor 
                  where time_mark=${withQuate(timeMark)} 
                    and category=${withQuate(category)} 
                    and source_name=${withQuate(sourceName)}""")
  }

  def getEntity(implicit conn: Connection): Option[DataMonitorRecord] = {
    import com.kent.util.Util._
    val queryStr = s"""select * from data_monitor 
                       where time_mark=${withQuate(timeMark)} 
                         and category=${withQuate(category)}
                         and source_name=${withQuate(sourceName)}
                    """
    val dmrOpt = querySql(queryStr, (rs: ResultSet) =>{
      if(rs.next()){
        val tm = rs.getString("time_mark")
  		  val categ = rs.getString("category")
  		  val sn = rs.getString("source_name")
  		  val num = rs.getDouble("num")
  		  val remark = rs.getString("REMARK")
  		  val wfiId = rs.getString("workflow_instance_id")
  		  val min = if(rs.getString("min") == null) Some(rs.getDouble("min")) else None
  		  val max = if(rs.getString("max") == null) Some(rs.getDouble("max")) else None
        DataMonitorRecord(tm, categ, sn, num, min, max, remark, wfiId)
      }else{
        null
      }
    })
    dmrOpt
  }

  def save(implicit conn: Connection): Boolean = {
    import com.kent.util.Util._
    val minStr = if(minOpt.isDefined) minOpt.get.toString() else null
    val maxStr = if(maxOpt.isDefined) maxOpt.get.toString() else null
    val insertSql = s"""
  	     insert into data_monitor values(${withQuate(timeMark)},${withQuate(category)},${withQuate(sourceName)},
  	     ${withQuate(num.toString())},${withQuate(minStr)},${withQuate(maxStr)},${withQuate(wfiId)},${withQuate(remark)})
  	    """
  	val updateSql = s""" 
  	    update data_monitor set num = ${withQuate(num.toString())}, 
  	                            min = ${withQuate(minStr)},
  	                            max = ${withQuate(maxStr)},
  	                            workflow_instance_id = ${withQuate(wfiId)},
  	                            remark = ${withQuate(remark)}
  	    where time_mark=${withQuate(timeMark)} 
          and category=${withQuate(category)}
          and source_name=${withQuate(sourceName)}
  	  """
    try {
      if(this.getEntity.isEmpty) executeSql(insertSql) else executeSql(updateSql)
    }catch{
      case e: SQLException => 
        e.printStackTrace()
        false
    }
  }
}
object DataMonitorRecord{
  def apply(timeMark: String, category: String, sourceName: String, num:Double, min: Option[Double], max: Option[Double], remark: String, wfiId: String):DataMonitorRecord = 
    new DataMonitorRecord(timeMark, category, sourceName, num, min, max, remark, wfiId)
}