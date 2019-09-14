package com.kent.daemon

import akka.actor.Cancellable
import com.kent.cron.task.{GenericTask, PlanTask, ResetTask}
import com.kent.pub.Event.{Start, Tick}
import com.kent.pub.Result
import com.kent.pub.actor.Daemon

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * @author kent
  * @date 2019-07-14
  * @desc
  *
  **/
class CronRunner extends Daemon{
  var scheduler:Cancellable = _
  val interval = 5

  val tasks = scala.collection.mutable.ArrayBuffer[GenericTask]()

  override def preStart(): Unit = {
    super.preStart()

    val resetCron = context.system.settings.config.getString("workflow.cron-runner.reset")
    val planCron = context.system.settings.config.getString("workflow.cron-runner.plan")
    //重置任务
    val resetTask = ResetTask(resetCron)
    val planTask = PlanTask(planCron)
    tasks += resetTask
    tasks += planTask
    //xxx

  }

  /**
    * 私有消息事件处理
    */
  override def individualReceive: Receive = {
    case Start() => this.start()
    case Tick() => this.tick()
  }

  def tick(): Unit = {
    tasks.foreach{
      case x if x.isReady() =>
        x.execute().map{ y =>
          log.info(y.message)
        }
      case _ =>
    }
  }

  def start(): Unit = {
    this.scheduler = context.system.scheduler.schedule(2000 millis, interval seconds){
      self ! Tick()
    }
  }
}

object CronRunner {
  def apply(): CronRunner = new CronRunner()
}
