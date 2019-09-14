package com.kent.cron.task
import com.kent.main.Master
import com.kent.pub.Event._
import com.kent.pub._

import scala.concurrent.ExecutionContext.Implicits.global
import akka.pattern.{ask, pipe}

import scala.concurrent.Future
import scala.concurrent.Future

/**
  * @author kent
  * @date 2019-07-21
  * @desc
  *
  **/
class PlanTask(cron: String) extends GenericTask(cron) {
  /**
    * 执行逻辑
    *
    * @return
    */
  override def execute(): Future[Result] = {
    val resultF = (Master.workflowManager ? GetTodayAllLeftTriggerCnt()).mapTo[Result]
    resultF.recover{
      case e: Exception => FailedResult(e.getMessage)
    }
  }
}

object PlanTask {
  def apply(cronStr: String): PlanTask = new PlanTask(cronStr)
}
