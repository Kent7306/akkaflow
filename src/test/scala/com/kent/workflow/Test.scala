package com.kent.workflow

import scala.sys.process._

object Test extends App{
  val pLogger = ProcessLogger(line => println("INFO："+line),line => println(s"ERROR: ${line}"))
    val process = Process("D:/Strawberry/perl/bin/perl F:/test.pl")
    val result = process.run(pLogger)
     //pLogger
    println("==========="+result.exitValue()+"*****************") 
} 