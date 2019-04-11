package com.kent.test

object AA extends App{
  val l = List(1,5,2,7,4,9,10)
  val l22 = List(1,5,2,7,4,9)
  val l2 = l.sortWith{case(a,b) => a < b}
  println(l2)
  l.reduceLeft{(a,b) => a+b}
  
  val l3 = l.groupBy { x => x % 3 }
  println(l3)
  
  println(l.take(4))
  println(l.takeRight(4))
  
  println(l.partition { x => x % 2 == 0 })
  println(l zip l22)
  println(l.slice(3, 7))
  println(l.updated(2, 999))
  println(l.dropWhile { x => x == 2 }+"----")
  
  //(0 until l.size).foreach(x => println(x))
  
  val str = "2016-12-12 12:12:12 INFO [Main] xxxxxxxxxxxxx.HTTPxxxx2016-12-13 12:12:12 INFO [Main] xxxxxxxxxxxxx.HTTPxxxx"
  
  val regx = "(\\d{4}-\\d{2}-\\d{2})\\s+(.+?)\\s+(\\[.+?\\])".r
  val matches = regx.findAllIn(str)
  while(matches.hasNext){
    matches.next()
    println(matches.group(1))
  }  
  
  val b = "jdbc:mysql://xxx:24/wf"
	val rgx = ":\\d+/([^\\?]+)".r
  val ms = rgx.findAllIn(b)
  if(ms.hasNext){
    ms.next()
    println(ms.group(1))
  }  
}