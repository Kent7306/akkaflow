package com.kent.test

import cronish.Cron

object TestCron extends App{
  val pattern = """(?i)every""".r
  val cron = Cron("0", "0", "1", "*", "*", "*" , "*")
  println(cron.nextTime)
  println(cron.nextTime)
  println(cron.nextTime)
}