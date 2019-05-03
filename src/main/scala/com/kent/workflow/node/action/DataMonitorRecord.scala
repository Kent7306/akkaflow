package com.kent.workflow.node.action

import com.kent.pub.DeepCloneable

/**
  * 数据监控记录器
  * @param sdate
  * @param category
  * @param sourceName
  * @param num
  * @param minOpt
  * @param maxOpt
  * @param remark
  * @param wfiId
  */
class DataMonitorRecord(val sdate: String, val category: String, val sourceName: String, val num:Double, val minOpt: Option[Double], val maxOpt: Option[Double],val remark: String, val wfiId: String)
       extends DeepCloneable[DataMonitorRecord] {
}
object DataMonitorRecord{
  def apply(timeMark: String, category: String, sourceName: String, num:Double, min: Option[Double], max: Option[Double], remark: String, wfiId: String):DataMonitorRecord = 
    new DataMonitorRecord(timeMark, category, sourceName, num, min, max, remark, wfiId)
}