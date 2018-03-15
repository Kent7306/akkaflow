package com.kent.main

/**
 * 伪分布启动（测试）
 */
object Standalone extends App{
  Master.main(Array())
  Thread.sleep(5000)
  HttpServer.main(Array())
  Thread.sleep(5000)
  Worker.main(Array())
}