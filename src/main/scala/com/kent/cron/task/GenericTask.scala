package com.kent.cron.task

import java.text.SimpleDateFormat

import akka.util.Timeout
import com.kent.pub.Result
import com.kent.util.Util
import com.kent.workflow.CronComponent

import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * @author kent
  * @date 2019-07-14
  * @desc 通用cron任务
  *
  **/
abstract class GenericTask(cronStr: String) {
  implicit val timeout = Timeout(30 seconds)

  val sbt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val startDate = sbt.parse("1990-12-29 00:00:00")
  val endDate = sbt.parse("2090-12-29 00:00:00")
  var cron: CronComponent = CronComponent(cronStr, startDate, endDate)

  /**
    * 执行逻辑
    * @return
    */
  def execute(): Future[Result]

  /**
    * 是否可以执行
    * @return
    */
  def isReady(): Boolean = {
    if (cron.isAfterExecuteTime){
      cron.setNextExecuteTime()
      true
    } else {
      false
    }
  }

}
