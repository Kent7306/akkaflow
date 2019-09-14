package com.kent.workflow

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.kent.pub.DeepCloneable
import com.kent.util.Util
import cronish._
import cronish.dsl._
import scalendar._

class CronComponent(private var _cronStr: String, private var _sdate: Date, private var _edate: Date) extends DeepCloneable[CronComponent] {
  //下一次执行时间
  var nextExecuteTime: Date = _
  def cronStr = _cronStr
  var sdate:Date = _sdate
  var edate: Date = _edate
  
  val pattern = """(?i)every""".r
  @transient val cron = if(pattern.findFirstIn(_cronStr).isEmpty){
                val elements = _cronStr.split(" ")
                Cron("0", elements(0), elements(1), elements(2), elements(3), elements(4), "*")
              } else {                
            	  cronStr.cron    
              }
  setNextExecuteTime()
  /**
   * 在当前时间下，下一次的执行时间
   */
  def setNextExecuteTime(): Boolean = {
    val now = Util.nowDate
    if(now.getTime > _sdate.getTime && now.getTime < _edate.getTime){
      this.nextExecuteTime = cron.nextTime.date
    	true
    }else{
      false
    }
  }

  /**
    * 得到今日内剩余的执行周期个数
    * @return
    */
  def getTodayExecuteLeftCnt():Int = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val nowFormat = dateFormat.format(Util.nowDate)
    var nextFormat: String = null
    var next: Date = Util.nowDate
    var i: Int = -1
    do {
      i = i + 1
      next = this.calculateNextTime(next)
      nextFormat = dateFormat.format(next)
    } while (nowFormat == nextFormat)
    i
  }

  /**
    * 计算指定时间情况下，下一次执行时间
    * @param date
    * @return
    */
  def calculateNextTime(date: Date): Date = {
    val t = cron.nextFrom(Scalendar(date.getTime))
    val nt = date.getTime + t
    new Date(nt)
  }
  /**
   * 当前是否处于执行时间范围前
   */
  def isBeforeTimeRange(): Boolean = if(Util.nowDate.getTime < _sdate.getTime) true else false
  /**
   * 当前是否处于执行时间范围后
   */
  def isAfterTimeRange(): Boolean = if(Util.nowDate.getTime > _edate.getTime) true else false
  /**
   * 是否可以执行了
   */
  def isAfterExecuteTime: Boolean = if(Util.nowDate.getTime > this.nextExecuteTime.getTime)true else false

  override def deepClone: CronComponent = {
    val tmp = new CronComponent(cronStr, sdate, edate)
    tmp.nextExecuteTime = nextExecuteTime
    tmp
  }
}

object CronComponent{
  def apply(_cronStr: String, _sdate: Date, _edate: Date): CronComponent = {
    if(_cronStr != null) new CronComponent(_cronStr, _sdate, _edate) else null
  }
  
  def main(args: Array[String]): Unit = {
    //val a = new CronComponent("Every day at midnight",Scalendar(2016,10, 11),Scalendar(2016,11, 11))
    val a = CronComponent("0 */1 1 * *",Scalendar(2016,10, 11),Scalendar(2019,11, 11))
    println(a.nextExecuteTime)
    println(a.getTodayExecuteLeftCnt())

    val tt = Scalendar(2019,4,1,17,11,11)


    println(a.calculateNextTime(tt))

  }
  
}