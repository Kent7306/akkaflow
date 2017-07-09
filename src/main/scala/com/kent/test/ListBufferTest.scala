package com.kent.test

import scala.collection.mutable.ListBuffer

object ListBufferTest extends App{
  val l = ListBuffer[String]()
  l += "111"
  l += "222"
  l += "333"
  l -= "333"
  
  val b = List("444","555")
  println(l)  
  l ++= b
  println(l +: b)
}