package com.kent.pub.dao

import com.kent.daemon.LogRecorder.LogMsg
import com.kent.util.Util._

/**
  * @author kent
  * @date 2019-07-01
  * @desc 日志数据库操作对象
  *
  **/
object LogRecordDao extends TransationManager with Daoable {
  def wq = withQuate _

  /**
    * 保存日志
    * @param record
    * @return
    */
  def save(record: LogMsg): Boolean = {
    transaction{
      val sql = s"insert into log_record values(null,${wq(record.level)},${wq(record.time)},${wq(record.ctype.toString)},${wq(record.sid)},${wq(record.name)},${wq(record.content)})"
      execute(sql)
    }
  }

  /**
    * 获取某个工作流实例的日志
    * @param wfiId
    * @return
    */
  def getLogWithWorkflowInstanceId(wfiId: String): List[List[String]] = {
    val sql = s"""
    select level,stime,name,content from log_record where sid = ${wq(wfiId)} order by stime
    """
    transaction{
      query[List[List[String]]](sql, rs => {
        var rowList = List[List[String]]()
        val colCnt = rs.getMetaData.getColumnCount
        while(rs.next()){
          val row = (1 to colCnt by 1).map { x => rs.getString(x) }.toList
          rowList = rowList :+ row
        }
        rowList
      }).getOrElse(List())
    }
  }

}
