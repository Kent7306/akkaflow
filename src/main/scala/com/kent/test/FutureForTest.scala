package com.kent.test

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
object FutureForTest extends App{
  val f1 = Future{println("f1111");Thread.sleep(3000);println("f1");true}
  val f2 = Future{println("f2");false}
  val e = for{
    a <- f1
    b <- f2
  } yield (a,b)
  
  Thread.sleep(20000)
}