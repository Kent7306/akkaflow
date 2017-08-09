package com.kent.test

import scala.sys.process._

object TheadTest extends App{
  def f(){
	  val pLogger = ProcessLogger(line => println(line),line => println(line))
		val executeResult = Process("ping -t 127.0.0.1").run(pLogger)
    
  }
  val t = new Thread(new Runnable() {
  	def run() {
  		f()
  	}
  })
  t.start()
  Thread.sleep(4000)
  t.interrupt()
}