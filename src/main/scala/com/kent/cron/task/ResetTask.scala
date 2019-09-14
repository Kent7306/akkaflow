package com.kent.cron.task
import com.kent.main.Master
import com.kent.pub.Event.ResetAllWorkflow
import com.kent.pub._

import scala.concurrent.ExecutionContext.Implicits.global
import akka.pattern.{ask, pipe}

import scala.concurrent.Future

/**
  * @author kent
  * @date 2019-07-14
  * @desc 定时重置所有workflow的任务
  *
  **/
class ResetTask(cronStr: String) extends GenericTask(cronStr) {

  override def execute(): Future[Result] = {
    val resultF = (Master.workflowManager ? ResetAllWorkflow()).mapTo[Result]
    resultF.recover{
      case e: Exception => FailedResult(e.getMessage)
    }
  }
}

object ResetTask {
  def apply(cronStr: String): ResetTask = new ResetTask(cronStr)
}
