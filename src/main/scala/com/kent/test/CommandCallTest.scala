package com.kent.test

import scala.sys.process._
import scala.collection.JavaConversions._

object CommandCallTest extends App {
  val pLogger = ProcessLogger(line => println(line),
    line => println(line))
  val executeResult = ("sh /etc/profile" #&& "echo $PATH").run(pLogger)
  //val executeResult = Process("echo $PATH").run(pLogger)
  if (executeResult.exitValue() == 0) true else false

  val m = System.getenv
  //println(System.getenv("AKKA_TEST"))
  System.setProperty("AKKA_TEST", "99999999")
 //System.setProperty("PATH", value)
  //m.map(x => println(x._1, x._2))

}